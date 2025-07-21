package ai.onehouse.metadata_extractor;

import ai.onehouse.RuntimeModule;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.storage.AsyncStorageClient;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.config.Config;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;

@Slf4j
public class TableDiscoveryAndUploadJob {
  private final TableDiscoveryService tableDiscoveryService;
  private final TableMetadataUploaderService tableMetadataUploaderService;
  private final ScheduledExecutorService scheduler;
  private final Object lock = new Object();
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  private final AsyncStorageClient asyncStorageClient;

  private Set<Table> tablesToProcess;
  private Instant previousTableMetadataUploadRunStartTime = Instant.EPOCH;
  private final Instant firstCronRunStartTime;

  @Inject
  public TableDiscoveryAndUploadJob(
      @Nonnull TableDiscoveryService tableDiscoveryService,
      @Nonnull TableMetadataUploaderService tableMetadataUploaderService,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics,
      @Nonnull @RuntimeModule.TableDiscoveryObjectStorageAsyncClient AsyncStorageClient asyncStorageClient) {
    this.scheduler = getScheduler();
    this.tableDiscoveryService = tableDiscoveryService;
    this.tableMetadataUploaderService = tableMetadataUploaderService;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
    this.firstCronRunStartTime = Instant.now();
    this.asyncStorageClient = asyncStorageClient;
  }

  /*
   * runs discovery and upload periodically at fixed intervals in a continuous fashion
   */
  public void runInContinuousMode(Config config) {
    log.debug("Running metadata-extractor in continuous mode");
    asyncStorageClient.initializeClient();
    // Schedule table discovery
    scheduler.scheduleAtFixedRate(
        this::discoverTables,
        0,
        config.getMetadataExtractorConfig().getTableDiscoveryIntervalMinutes(),
        TimeUnit.MINUTES);

    // Schedule table processing
    scheduler.scheduleAtFixedRate(
        () -> processTables(config),
        0,
        config.getMetadataExtractorConfig().getProcessTableMetadataSyncDurationSeconds(),
        TimeUnit.SECONDS);
  }

  /*
   * Runs table discovery followed by metadata uploader once
   */
  public void runOnce() {
    asyncStorageClient.initializeClient();
    runOnce(null, 1);
  }

  public void runOnce(Config config) {
    asyncStorageClient.initializeClient();
    runOnce(config, 1);
  }

  public void runOnce(Config config, int runCounter) {
    log.info("Running metadata-extractor starting at: {}", firstCronRunStartTime);
    Boolean isSucceeded =
        tableDiscoveryService
            .discoverTables()
            .thenCompose(tableMetadataUploaderService::uploadInstantsInTables)
            .join();
    if (Boolean.TRUE.equals(isSucceeded)) {
      log.info("Run Completed");
    } else {
      log.error("Run failed");
      /*
      * The retry is done in following known scenarios:
      * 1. Session token expiry for pull model customer
      * 2. Temporary network issues, sometimes external api call fails despite client retries
      * 3, Issues related to throttling of calls to S3/GCS
      * */
      if (config != null &&
          config.getMetadataExtractorConfig().getJobRunMode().equals(MetadataExtractorConfig.JobRunMode.ONCE_WITH_RETRY)
          && shouldRunAgainForRunOnceConfiguration(config)
          && runCounter < config.getMetadataExtractorConfig().getMaxRunCountForPullModel()) {
        log.info("Retrying job: Attempt {}/{}",
            runCounter + 1,
            config.getMetadataExtractorConfig().getMaxRunCountForPullModel());
        // Handle client session timeout errors if any
        asyncStorageClient.refreshClient();
        runOnce(config, runCounter + 1);
      }
    }
  }

  @VisibleForTesting
  boolean shouldRunAgainForRunOnceConfiguration(Config config) {
    MetadataExtractorConfig metadataExtractorConfig = config.getMetadataExtractorConfig();
    Cron cron = new CronParser((CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)))
        .parse(metadataExtractorConfig.getCronScheduleForPullModel());
    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    Optional<ZonedDateTime> nextExecutionTime =
        executionTime.nextExecution(firstCronRunStartTime.atZone(ZoneOffset.UTC));
    if (nextExecutionTime.isPresent() && Duration.between(firstCronRunStartTime,
        nextExecutionTime.get().toInstant()).toMinutes() < metadataExtractorConfig.getMinIntervalMinutes()) {
      log.info("Stopping the job as next scheduled run is less than 10 minutes away");
      return false;
    }
    return true;
  }

  private void discoverTables() {
    log.info("Discovering tables in provided paths");
    tableDiscoveryService
        .discoverTables()
        .thenApply(
            tables -> {
              synchronized (lock) {
                tablesToProcess = tables;
              }
              hudiMetadataExtractorMetrics.setDiscoveredTablesPerRound(tables.size());
              return null;
            })
        .exceptionally(
            ex -> {
              log.error("Error discovering tables: ", ex);
              hudiMetadataExtractorMetrics
                  .incrementTableDiscoveryFailureCounter(getMetadataExtractorFailureReason(
                  ex,
                  MetricsConstants.MetadataUploadFailureReasons.UNKNOWN));
              return null;
            })
        .join();
  }

  private void processTables(Config config) {
    log.debug("Polling to see if metadata needs to be uploaded");
    Instant tableMetadataUploadRunStartTime = Instant.now();
    if (Duration.between(previousTableMetadataUploadRunStartTime, tableMetadataUploadRunStartTime)
            .toMinutes()
        >= config.getMetadataExtractorConfig().getTableMetadataUploadIntervalMinutes()) {
      Set<Table> tables = null;
      synchronized (lock) {
        if (tablesToProcess != null) {
          tables = new HashSet<>(tablesToProcess);
        }
      }
      if (tables != null && !tables.isEmpty()) {
        log.debug("Uploading table metadata for discovered tables");
        hudiMetadataExtractorMetrics.resetTableProcessedGauge();
        AtomicBoolean hasError = new AtomicBoolean(false);
        tableMetadataUploaderService
            .uploadInstantsInTables(tables)
            .exceptionally(
                ex -> {
                  log.error("Error uploading instants in tables: ", ex);
                  hasError.set(true);
                  hudiMetadataExtractorMetrics.incrementTableSyncFailureCounter();
                  return null;
                })
            .join();
        if (!hasError.get()) {
          hudiMetadataExtractorMetrics.incrementTableSyncSuccessCounter();
        }
        previousTableMetadataUploadRunStartTime = tableMetadataUploadRunStartTime;
      }
    }
  }

  public void shutdown() {
    scheduler.shutdown();
  }

  @VisibleForTesting
  ScheduledExecutorService getScheduler() {
    return Executors.newScheduledThreadPool(2);
  }
}
