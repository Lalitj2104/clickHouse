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

    @Override
    public List<String> getClickHouseTables(ClickHouseConfig config) {
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection(config)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(config.getDatabase(), null, "%", new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        } catch (Exception e) {
            log.error("Error fetching tables: ", e);
            throw new RuntimeException("Failed to fetch tables", e);
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
        String columns = String.join(", ", config.getSelectedColumns());
        String query = String.format("SELECT %s FROM %s LIMIT %d", columns, tableName, limit);

        try (Connection conn = getConnection(config); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                preview.add(row);
            }
        } catch (Exception e) {
            log.error("Error previewing data: ", e);
            throw new RuntimeException("Failed to preview data", e);
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
        try (Connection conn = getConnection(config); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query); BufferedWriter writer = new BufferedWriter(new FileWriter(fileConfig.getFileName()))) {

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
        try (BufferedReader reader = new BufferedReader(new FileReader(fileConfig.getFileName())); Connection conn = getConnection(config)) {

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

    private Connection getConnection(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        properties.setProperty("password", config.getJwtToken());

        String url = String.format("jdbc:clickhouse://%s:%d/%s",
                config.getHost(),
                config.getPort(),
                config.getDatabase());

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        return dataSource.getConnection();
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
