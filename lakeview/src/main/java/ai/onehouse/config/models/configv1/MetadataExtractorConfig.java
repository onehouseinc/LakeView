package ai.onehouse.config.models.configv1;

import static ai.onehouse.constants.MetadataExtractorConstants.DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;
import static ai.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE;
import static ai.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE;
import static ai.onehouse.constants.MetadataExtractorConstants.PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;
import static ai.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static ai.onehouse.constants.MetadataExtractorConstants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;
import static ai.onehouse.constants.MetadataExtractorConstants.WAIT_TIME_BEFORE_SHUTDOWN;

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
  // This is used to estimate next run time for the job in pull model
  @Builder.Default private String cronScheduleForPullModel = "0 */1 * * *";
  @Builder.Default private Integer maxRunCountForPullModel = 5;
  @Builder.Default private Integer minIntervalMinutes = 10;

  @Builder.Default
  private UploadStrategy uploadStrategy = UploadStrategy.BLOCK_ON_INCOMPLETE_COMMIT;

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

  @Builder.Default private int fileUploadStreamBatchSize = DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;

  @Builder.Default private int waitTimeBeforeShutdown = WAIT_TIME_BEFORE_SHUTDOWN;

  @Builder.Default private int objectStoreNumRetries = 10;

  @Builder.Default private int nettyMaxConcurrency = 50;

  @Builder.Default private long nettyConnectionTimeoutSeconds = 60L;

  public enum JobRunMode {
    CONTINUOUS,
    ONCE,
    ONCE_WITH_RETRY
  }

  public enum UploadStrategy {
    BLOCK_ON_INCOMPLETE_COMMIT,
    CONTINUE_ON_INCOMPLETE_COMMIT
  }
}
