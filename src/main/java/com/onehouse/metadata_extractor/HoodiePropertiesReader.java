package com.onehouse.metadata_extractor;

import static com.onehouse.metadata_extractor.Constants.HOODIE_TABLE_NAME_KEY;
import static com.onehouse.metadata_extractor.Constants.HOODIE_TABLE_TYPE_KEY;

import com.google.inject.Inject;
import com.onehouse.api.request.TableType;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HoodiePropertiesReader {
  private final AsyncStorageClient asyncStorageClient;

  @Inject
  public HoodiePropertiesReader(AsyncStorageClient asyncStorageClient) {
    this.asyncStorageClient = asyncStorageClient;
  }

  public CompletableFuture<ParsedHudiProperties> readHoodieProperties(String path) {
    log.debug("parsing {} file", path);
    return asyncStorageClient
        .readFileAsInputStream(path)
        .thenApplyAsync(
            inputStream -> {
              Properties properties = new Properties();
              try {
                properties.load(inputStream);
                inputStream.close();
              } catch (IOException e) {
                throw new RuntimeException("Failed to load properties file", e);
              }
              return ParsedHudiProperties.builder()
                  .tableName(properties.getProperty(HOODIE_TABLE_NAME_KEY))
                  .tableType(TableType.valueOf(properties.getProperty(HOODIE_TABLE_TYPE_KEY)))
                  .build();
            });
  }
}
