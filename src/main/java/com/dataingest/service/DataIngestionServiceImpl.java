package com.dataingest.service;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.dataingest.model.ClickHouseConfig;
import com.dataingest.model.FlatFileConfig;
import com.dataingest.model.IngestionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DataIngestionServiceImpl implements DataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(DataIngestionServiceImpl.class);

    private Connection getConnection(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        properties.setProperty("password", config.getJwtToken());
        properties.setProperty("ssl", "true");
        properties.setProperty("sslmode", "STRICT");
        properties.setProperty("compress", "0"); // Disable compression
        properties.setProperty("use_client_time_zone", "true");
        
        // Using HTTPS protocol for secure connection
        String url = String.format("jdbc:clickhouse://%s:%d/%s",
                config.getHost(),
                config.getPort(),
                config.getDatabase());

        log.info("Attempting to connect with URL: {} and user: {}", url, config.getUser());
        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
            Connection conn = dataSource.getConnection();
            log.info("Successfully connected to ClickHouse");
            return conn;
        } catch (SQLException e) {
            log.error("Failed to connect to ClickHouse: {} - {}", e.getErrorCode(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public List<String> getClickHouseTables(ClickHouseConfig config) {
        List<String> tables = new ArrayList<>();
        String query = "SELECT name FROM system.tables WHERE database = ?";
        
        try (Connection conn = getConnection(config);
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            stmt.setString(1, config.getDatabase());
            log.info("Executing query: {} with database: {}", query, config.getDatabase());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    tables.add(tableName);
                    log.info("Found table: {}", tableName);
                }
            }
            
            if (tables.isEmpty()) {
                log.warn("No tables found in database: {}", config.getDatabase());
            } else {
                log.info("Found {} tables: {}", tables.size(), String.join(", ", tables));
            }
            
        } catch (SQLException e) {
            log.error("SQL Error fetching tables - Code: {} Message: {}", e.getErrorCode(), e.getMessage(), e);
            throw new RuntimeException("Failed to fetch tables: " + e.getMessage(), e);
        }
        return tables;
    }

    @Override
    public List<String> getTableColumns(ClickHouseConfig config, String tableName) {
        List<String> columns = new ArrayList<>();
        try (Connection conn = getConnection(config)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(config.getDatabase(), null, tableName, "%");
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (Exception e) {
            log.error("Error fetching columns: ", e);
            throw new RuntimeException("Failed to fetch columns", e);
        }
        return columns;
    }

    @Override
    public List<Map<String, Object>> previewData(ClickHouseConfig config, String tableName, int limit) {
        List<Map<String, Object>> preview = new ArrayList<>();
        if (config.getSelectedColumns() == null || config.getSelectedColumns().length == 0) {
            throw new IllegalArgumentException("No columns selected for preview");
        }
        
        String columns = String.join(", ", config.getSelectedColumns());
        config.setSelectedTables(new String[]{tableName}); // Set the selected table
        
        String query = String.format("SELECT %s FROM %s LIMIT %d", columns, tableName, limit);
        log.info("Executing preview query: {}", query);

        try (Connection conn = getConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    // Convert null values to empty string for consistent display
                    row.put(columnName, value != null ? value.toString() : "");
                }
                preview.add(row);
            }
        } catch (Exception e) {
            log.error("Error previewing data: ", e);
            throw new RuntimeException("Failed to preview data: " + e.getMessage(), e);
        }
        return preview;
    }

    @Override
    public IngestionResult ingestFromClickHouseToFile(ClickHouseConfig config, FlatFileConfig fileConfig) {
        String columns = String.join(", ", config.getSelectedColumns());
        String query;
        if (config.getSelectedTables().length > 1) {
            query = buildJoinQuery(config);
        } else {
            query = String.format("SELECT %s FROM %s", columns, config.getSelectedTables()[0]);
        }

        long recordCount = 0;
        try (Connection conn = getConnection(config); 
             Statement stmt = conn.createStatement(); 
             ResultSet rs = stmt.executeQuery(query); 
             BufferedWriter writer = new BufferedWriter(new FileWriter(fileConfig.getFileName()))) {

            // Write header
            if (fileConfig.isHasHeader()) {
                writer.write(String.join(fileConfig.getDelimiter(), config.getSelectedColumns()));
                writer.newLine();
            }

            // Write data
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (String column : config.getSelectedColumns()) {
                    Object value = rs.getObject(column);
                    values.add(value != null ? value.toString() : "");
                }
                writer.write(String.join(fileConfig.getDelimiter(), values));
                writer.newLine();
                recordCount++;
            }

            return IngestionResult.builder()
                    .success(true)
                    .recordsProcessed(recordCount)
                    .message("Data successfully exported to file")
                    .build();

        } catch (Exception e) {
            log.error("Error during ingestion to file: ", e);
            return IngestionResult.builder()
                    .success(false)
                    .recordsProcessed(recordCount)
                    .message("Failed to export data")
                    .errorDetails(e.getMessage())
                    .build();
        }
    }

    @Override
    public IngestionResult ingestFromFileToClickHouse(FlatFileConfig fileConfig, ClickHouseConfig config) {
        long recordCount = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(fileConfig.getFileName())); 
             Connection conn = getConnection(config)) {

            // Skip header if present
            if (fileConfig.isHasHeader()) {
                reader.readLine();
            }

            // Create table if not exists
            String createTableSql = buildCreateTableSql(fileConfig, config);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSql);
            }

            // Prepare insert statement
            String insertSql = buildInsertSql(config);
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(fileConfig.getDelimiter(), -1);
                    for (int i = 0; i < values.length; i++) {
                        pstmt.setString(i + 1, values[i]);
                    }
                    pstmt.addBatch();
                    recordCount++;

                    if (recordCount % 1000 == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }

            return IngestionResult.builder()
                    .success(true)
                    .recordsProcessed(recordCount)
                    .message("Data successfully imported to ClickHouse")
                    .build();

        } catch (Exception e) {
            log.error("Error during ingestion to ClickHouse: ", e);
            return IngestionResult.builder()
                    .success(false)
                    .recordsProcessed(recordCount)
                    .message("Failed to import data")
                    .errorDetails(e.getMessage())
                    .build();
        }
    }

    private long getTotalRecordCount(ClickHouseConfig config) {
        String countQuery;
        if (config.getSelectedTables().length > 1) {
            countQuery = "SELECT COUNT(*) FROM " + config.getSelectedTables()[0] + " " +
                    String.join(" ", Arrays.stream(config.getSelectedTables())
                            .skip(1)
                            .map(table -> "JOIN " + table + " ON " + config.getJoinCondition())
                            .collect(Collectors.toList()));
        } else {
            countQuery = "SELECT COUNT(*) FROM " + config.getSelectedTables()[0];
        }

        try (Connection conn = getConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            log.error("Error getting total record count: ", e);
        }
        return 0;
    }

    private long countFileLines(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            return reader.lines().count();
        } catch (Exception e) {
            log.error("Error counting file lines: ", e);
            return 0;
        }
    }

    private String buildJoinQuery(ClickHouseConfig config) {
        String columns = String.join(", ", config.getSelectedColumns());
        String mainTable = config.getSelectedTables()[0];
        StringBuilder query = new StringBuilder(String.format("SELECT %s FROM %s", columns, mainTable));

        for (int i = 1; i < config.getSelectedTables().length; i++) {
            query.append(String.format(" JOIN %s ON %s",
                    config.getSelectedTables()[i],
                    config.getJoinCondition()));
        }

        return query.toString();
    }

    private String buildCreateTableSql(FlatFileConfig fileConfig, ClickHouseConfig config) {
        String columns = Arrays.stream(fileConfig.getSelectedColumns())
                .map(col -> String.format("%s String", col))
                .collect(Collectors.joining(", "));

        return String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
                config.getSelectedTables()[0], columns);
    }

    private String buildInsertSql(ClickHouseConfig config) {
        String columns = String.join(", ", config.getSelectedColumns());
        String placeholders = String.join(", ", Collections.nCopies(config.getSelectedColumns().length, "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                config.getSelectedTables()[0], columns, placeholders);
    }
}
