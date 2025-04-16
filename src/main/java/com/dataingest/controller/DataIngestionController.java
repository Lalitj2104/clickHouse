package com.dataingest.controller;

import com.dataingest.model.ClickHouseConfig;
import com.dataingest.model.FlatFileConfig;
import com.dataingest.model.IngestionResult;
import com.dataingest.service.DataIngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Controller
public class DataIngestionController {
    private static final String EXPORT_DIR = "exports";

    @Autowired
    private DataIngestionService dataIngestionService;

    @GetMapping("/")
    public String showMainPage(Model model) {
        return "index";
    }

    @PostMapping("/tables")
    @ResponseBody
    public ResponseEntity<?> getTables(@RequestBody ClickHouseConfig config) {
        try {
            List<String> tables = dataIngestionService.getClickHouseTables(config);
            return ResponseEntity.ok(tables);
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body("Failed to fetch tables: " + e.getMessage());
        }
    }

    @PostMapping("/columns")
    @ResponseBody
    public List<String> getColumns(@RequestBody ClickHouseConfig config, @RequestParam String tableName) {
        return dataIngestionService.getTableColumns(config, tableName);
    }

    @PostMapping("/preview")
    @ResponseBody
    public List<Map<String, Object>> previewData(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName,
            @RequestParam(defaultValue = "100") int limit) {
        return dataIngestionService.previewData(config, tableName, limit);
    }

    @PostMapping("/preview/file")
    @ResponseBody
    public List<Map<String, Object>> previewFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam(defaultValue = ",") String delimiter,
            @RequestParam(defaultValue = "true") boolean hasHeader,
            @RequestParam(defaultValue = "10") int limit) throws IOException {
        
        List<Map<String, Object>> preview = new ArrayList<>();
        String[] headers;
        
        // First read to get headers
        try (BufferedReader headerReader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String headerLine = headerReader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("File is empty");
            }
            
            headers = headerLine.split(delimiter, -1);
            if (!hasHeader) {
                // If no header, use column1, column2, etc.
                headers = new String[headers.length];
                for (int i = 0; i < headers.length; i++) {
                    headers[i] = "column" + (i + 1);
                }
            }
        }

        // Second read to get data
        try (BufferedReader dataReader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            // Skip header if it exists
            if (hasHeader) {
                dataReader.readLine();
            }
            
            // Read data rows
            String line;
            int rowCount = 0;
            while ((line = dataReader.readLine()) != null && rowCount < limit) {
                String[] values = line.split(delimiter, -1);
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < Math.min(headers.length, values.length); i++) {
                    row.put(headers[i], values[i]);
                }
                preview.add(row);
                rowCount++;
            }
        }

        return preview;
    }

    @PostMapping("/ingest/clickhouse-to-file")
    @ResponseBody
    public ResponseEntity<IngestionResult> ingestToFile(
            @RequestBody ClickHouseConfig clickHouseConfig,
            @RequestParam String fileName,
            @RequestParam String delimiter,
            @RequestParam(defaultValue = "true") boolean hasHeader) throws IOException {

        // Create exports directory if it doesn't exist
        Path exportDir = Paths.get(EXPORT_DIR);
        if (!Files.exists(exportDir)) {
            Files.createDirectories(exportDir);
        }

        // Set the full file path
        String safeName = StringUtils.cleanPath(fileName);
        FlatFileConfig fileConfig = new FlatFileConfig();
        fileConfig.setFileName(exportDir.resolve(safeName).toString());
        fileConfig.setDelimiter(delimiter);
        fileConfig.setHasHeader(hasHeader);
        fileConfig.setSelectedColumns(clickHouseConfig.getSelectedColumns());

        IngestionResult result = dataIngestionService.ingestFromClickHouseToFile(clickHouseConfig, fileConfig);
        return ResponseEntity.ok(result);
    }

    @PostMapping(value = "/ingest/file-to-clickhouse", consumes = "multipart/form-data")
    @ResponseBody
    public ResponseEntity<IngestionResult> ingestToClickHouse(
            @RequestParam("file") MultipartFile file,
            @RequestParam String delimiter,
            @RequestParam(defaultValue = "true") boolean hasHeader,
            @RequestParam String config) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        
        try {
            ClickHouseConfig clickHouseConfig = mapper.readValue(config, ClickHouseConfig.class);
            
            if (file.isEmpty()) {
                return ResponseEntity.badRequest().body(
                    IngestionResult.builder()
                        .success(false)
                        .message("Please select a file")
                        .build()
                );
            }

            // Save uploaded file temporarily
            Path tempDir = Files.createTempDirectory("clickhouse-ingestion");
            File tempFile = new File(tempDir.toFile(), StringUtils.cleanPath(file.getOriginalFilename()));
            file.transferTo(tempFile);

            FlatFileConfig fileConfig = new FlatFileConfig();
            fileConfig.setFileName(tempFile.getAbsolutePath());
            fileConfig.setDelimiter(delimiter);
            fileConfig.setHasHeader(hasHeader);
            fileConfig.setSelectedColumns(clickHouseConfig.getSelectedColumns());

            try {
                IngestionResult result = dataIngestionService.ingestFromFileToClickHouse(fileConfig, clickHouseConfig);
                return ResponseEntity.ok(result);
            } finally {
                // Cleanup
                tempFile.delete();
                tempDir.toFile().delete();
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                IngestionResult.builder()
                    .success(false)
                    .message("Failed to process file: " + e.getMessage())
                    .build()
            );
        }
    }

    @GetMapping("/exports/{fileName:.+}")
    public ResponseEntity<Resource> downloadFile(@PathVariable String fileName) {
        try {
            Path filePath = Paths.get(EXPORT_DIR).resolve(fileName);
            Resource resource = new FileSystemResource(filePath.toFile());
            
            if (resource.exists()) {
                return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                    .body(resource);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
