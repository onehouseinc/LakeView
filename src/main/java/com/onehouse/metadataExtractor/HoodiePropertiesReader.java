package com.onehouse.metadataExtractor;

import com.google.inject.Inject;
import com.onehouse.storage.AsyncStorageReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class HoodiePropertiesReader {
  private final AsyncStorageReader asyncStorageReader;

  @Inject
  public HoodiePropertiesReader(AsyncStorageReader asyncStorageReader) {
    this.asyncStorageReader = asyncStorageReader;
  }

  public CompletableFuture<Map<String, String>> readHoodieProperties(String path) {
    return asyncStorageReader
        .readFile(path)
        .thenApplyAsync(
            inputStream -> {
              Properties properties = new Properties();
              try {
                properties.load(inputStream);
              } catch (IOException e) {
                throw new RuntimeException("Failed to load properties file", e);
              }
              Map<String, String> propertiesMap = new HashMap<>();
              propertiesMap.put("tableName", properties.getProperty("hoodie.table.name"));
              propertiesMap.put("tableType", properties.getProperty("hoodie.table.type"));
              return propertiesMap;
            });
  }
}
