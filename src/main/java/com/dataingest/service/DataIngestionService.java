package com.dataingest.service;

import com.dataingest.model.ClickHouseConfig;
import com.dataingest.model.FlatFileConfig;
import com.dataingest.model.IngestionResult;

import java.util.List;
import java.util.Map;

public interface DataIngestionService {

    List<String> getClickHouseTables(ClickHouseConfig config);

    List<String> getTableColumns(ClickHouseConfig config, String tableName);

    List<Map<String, Object>> previewData(ClickHouseConfig config, String tableName, int limit);

    IngestionResult ingestFromClickHouseToFile(ClickHouseConfig config, FlatFileConfig fileConfig);

    IngestionResult ingestFromFileToClickHouse(FlatFileConfig fileConfig, ClickHouseConfig config);
}
