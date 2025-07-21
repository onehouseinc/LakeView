package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_NAME_KEY;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_TYPE_KEY;
import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;

import com.google.inject.Inject;
import ai.onehouse.api.models.request.TableType;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metadata_extractor.models.ParsedHudiProperties;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.RuntimeModule.TableMetadataUploadObjectStorageAsyncClient;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HoodiePropertiesReader {
  private final AsyncStorageClient asyncStorageClient;
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;

  @Inject
  public HoodiePropertiesReader(
      @TableMetadataUploadObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      LakeViewExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
  }

  public CompletableFuture<ParsedHudiProperties> readHoodieProperties(String path) {
    log.debug("parsing {} file", path);
    return asyncStorageClient
        .streamFileAsync(path)
        .thenApplyAsync(
            fileStreamData -> {
              Properties properties = new Properties();
              try (InputStream is = fileStreamData.getInputStream()) {
                properties.load(is);
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
              hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
                getMetadataExtractorFailureReason(
                    throwable,
                    MetricsConstants.MetadataUploadFailureReasons.HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED)
              );
              return null;
            });
  }
}
