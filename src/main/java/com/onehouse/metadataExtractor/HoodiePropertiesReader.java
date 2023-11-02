package com.onehouse.metadataExtractor;

import com.google.inject.Inject;
import com.onehouse.api.request.TableType;
import com.onehouse.metadataExtractor.models.ParsedHudiProperties;
import com.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodiePropertiesReader {
  private final AsyncStorageClient asyncStorageClient;
  private static final Logger logger = LoggerFactory.getLogger(HoodiePropertiesReader.class);

  @Inject
  public HoodiePropertiesReader(AsyncStorageClient asyncStorageClient) {
    this.asyncStorageClient = asyncStorageClient;
  }

  public CompletableFuture<ParsedHudiProperties> readHoodieProperties(String path) {
    logger.debug(String.format("parsing %s file", path));
    return asyncStorageClient
        .readFileAsInputStream(path)
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
