package com.onehouse.config.models.configv1;

import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE;
import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;
import static com.onehouse.metadata_extractor.TableDiscoveryAndUploadJob.PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;

import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class MetadataExtractorConfig {
  @NonNull private List<ParserConfig> parserConfig;
  @Builder.Default private Optional<List<String>> pathExclusionPatterns = Optional.empty();
  @Builder.Default private JobRunMode jobRunMode = JobRunMode.CONTINUOUS;

  @Builder.Default
  private int presignedUrlRequestBatchSizeActiveTimeline =
      PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE;

  @Builder.Default
  private int presignedUrlRequestBatchSizeArchivedTimeline =
      PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE;

  @Builder.Default
  private int processTableMetadataSyncDurationSeconds =
      PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;

  @Builder.Default private int tableDiscoveryIntervalMinutes = TABLE_DISCOVERY_INTERVAL_MINUTES;

  @Builder.Default
  private int tableMetadataUploadIntervalMinutes = TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;

  public enum JobRunMode {
    CONTINUOUS,
    ONCE
  }
}
