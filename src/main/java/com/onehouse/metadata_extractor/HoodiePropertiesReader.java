package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_NAME_KEY;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_TYPE_KEY;

import com.google.inject.Inject;
import com.onehouse.api.models.request.TableType;
import com.onehouse.constants.MetricsConstants;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.metrics.HudiMetadataExtractorMetrics;
import com.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

@Slf4j
public class HoodiePropertiesReader {
  private final AsyncStorageClient asyncStorageClient;
  private final HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics;

  @Inject
  public HoodiePropertiesReader(AsyncStorageClient asyncStorageClient, HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
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
            })
        .exceptionally(
            throwable -> {
              log.error("Error encountered when reading hoodie properties file", throwable);
                hudiMetadataExtractorMetrics.incrementTableMetadataUploadFailureCounter(MetricsConstants.MetadataUploadFailureReasons.HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED);
              return null;
            });
  }
}
