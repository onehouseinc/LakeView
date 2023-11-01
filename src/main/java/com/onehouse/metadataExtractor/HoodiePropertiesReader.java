package com.onehouse.metadataExtractor;

import com.google.inject.Inject;
import com.onehouse.api.request.TableType;
import com.onehouse.metadataExtractor.models.ParsedHudiProperties;
import com.onehouse.storage.AsyncStorageReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class HoodiePropertiesReader {
  private final AsyncStorageReader asyncStorageReader;

  @Inject
  public HoodiePropertiesReader(AsyncStorageReader asyncStorageReader) {
    this.asyncStorageReader = asyncStorageReader;
  }

  public CompletableFuture<ParsedHudiProperties> readHoodieProperties(String path) {
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
              return ParsedHudiProperties.builder()
                  .tableName(properties.getProperty("hoodie.table.name"))
                  .tableType(TableType.valueOf(properties.getProperty("hoodie.table.type")))
                  .build();
            });
  }
}
