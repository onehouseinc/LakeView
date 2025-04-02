package ai.onehouse.metadata_extractor;

import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
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
import java.time.ZoneId;
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
      @Nonnull AsyncStorageClient asyncStorageClient) {
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
    runOnce(null, 1);
  }

  public void runOnce(Config config, int runCounter) {
    log.info("Running metadata-extractor one time starting at: {}", firstCronRunStartTime);
    Boolean isSucceeded =
        tableDiscoveryService
            .discoverTables()
            .thenCompose(tableMetadataUploaderService::uploadInstantsInTables)
            .join();
    if (Boolean.TRUE.equals(isSucceeded)) {
      log.info("Run Completed");
    } else {
      log.error("Run failed");
      if (config != null &&
          config.getMetadataExtractorConfig().getJobRunMode().equals(MetadataExtractorConfig.JobRunMode.ONCE_PULL_MODEL)
          && shouldRunAgainForRunOnceConfiguration(config)
          && runCounter < config.getMetadataExtractorConfig().getMaxRunCountForPullModel()) {
        log.info("Retrying job: Attempt {}/{} after waiting {} minutes",
            runCounter + 1,
            config.getMetadataExtractorConfig().getMaxRunCountForPullModel(),
            config.getMetadataExtractorConfig().getWaitTimeRetryForPullModel());
        try {
          Thread.sleep( config.getMetadataExtractorConfig().getWaitTimeRetryForPullModel() * 60 * 1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Retry wait interrupted, proceeding with retry immediately");
        }
        asyncStorageClient.refreshClient();
        runOnce(config, runCounter + 1);
      }
    }
  }

  @VisibleForTesting
  boolean shouldRunAgainForRunOnceConfiguration(Config config) {
    Cron cron = new CronParser((CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX))).parse(config.getMetadataExtractorConfig().getCronScheduleForPullModel());
    ExecutionTime executionTime = ExecutionTime.forCron(cron);
    Optional<ZonedDateTime> nextExecutionTime = executionTime.nextExecution(firstCronRunStartTime.atZone(ZoneOffset.UTC));
    if (nextExecutionTime.isPresent() && Duration.between(firstCronRunStartTime, nextExecutionTime.get().toInstant()).toMinutes() < 30) {
      log.info("Stopping the job as next scheduled run is less than 30 minutes away");
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
              hudiMetadataExtractorMetrics.incrementTableDiscoveryFailureCounter();
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
