package com.dataingest.controller;

import com.dataingest.model.ClickHouseConfig;
import com.dataingest.model.FlatFileConfig;
import com.dataingest.model.IngestionResult;
import com.dataingest.service.DataIngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Controller
public class DataIngestionController {

    @Autowired
    private DataIngestionService dataIngestionService;

    @GetMapping("/")
    public String showMainPage(Model model) {
        return "index";
    }

    @PostMapping("/tables")
    @ResponseBody
    public List<String> getTables(@RequestBody ClickHouseConfig config) {
        return dataIngestionService.getClickHouseTables(config);
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

    @PostMapping("/ingest/clickhouse-to-file")
    @ResponseBody
    public ResponseEntity<IngestionResult> ingestToFile(
            @RequestBody ClickHouseConfig clickHouseConfig,
            @RequestParam String fileName,
            @RequestParam String delimiter,
            @RequestParam(defaultValue = "true") boolean hasHeader) {

        FlatFileConfig fileConfig = new FlatFileConfig();
        fileConfig.setFileName(fileName);
        fileConfig.setDelimiter(delimiter);
        fileConfig.setHasHeader(hasHeader);
        fileConfig.setSelectedColumns(clickHouseConfig.getSelectedColumns());

        IngestionResult result = dataIngestionService.ingestFromClickHouseToFile(clickHouseConfig, fileConfig);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/ingest/file-to-clickhouse")
    @ResponseBody
    public ResponseEntity<IngestionResult> ingestToClickHouse(
            @RequestParam("file") MultipartFile file,
            @RequestParam String delimiter,
            @RequestParam(defaultValue = "true") boolean hasHeader,
            @RequestBody ClickHouseConfig clickHouseConfig) throws Exception {

        // Save uploaded file temporarily
        Path tempDir = Files.createTempDirectory("clickhouse-ingestion");
        File tempFile = new File(tempDir.toFile(), file.getOriginalFilename());
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
    }
}