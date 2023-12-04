package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.onehouse.metadata_extractor.models.Table;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableDiscoveryAndUploadJob {
  private final TableDiscoveryService tableDiscoveryService;
  private final TableMetadataUploaderService tableMetadataUploaderService;
  private final ScheduledExecutorService scheduler;
  private final Object lock = new Object();
  // process table metadata will be called every 30 seconds,
  // but metadata will be uploaded only if TABLE_METADATA_UPLOAD_INTERVAL_MINUTES amount of time has
  // passed since last run
  private static final int PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS = 30;
  private Set<Table> tablesToProcess;
  private Instant previousTableMetadataUploadRunStartTime = Instant.EPOCH;

  @Inject
  public TableDiscoveryAndUploadJob(
      @Nonnull TableDiscoveryService tableDiscoveryService,
      @Nonnull TableMetadataUploaderService tableMetadataUploaderService) {
    this.scheduler = getScheduler();
    this.tableDiscoveryService = tableDiscoveryService;
    this.tableMetadataUploaderService = tableMetadataUploaderService;
  }

  /*
   * runs discovery and upload periodically at fixed intervals in a continuous fashion
   */
  public void runInContinuousMode() {
    log.debug("Running metadata-extractor in continuous mode");
    // Schedule table discovery
    scheduler.scheduleAtFixedRate(
        this::discoverTables, 0, TABLE_DISCOVERY_INTERVAL_MINUTES, TimeUnit.MINUTES);

    // Schedule table processing
    scheduler.scheduleAtFixedRate(
        this::processTables, 0, PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS, TimeUnit.SECONDS);
  }

  /*
   * Runs table discovery followed by metadata uploader once
   */
  public void runOnce() {
    log.info("Current pid: {}", ProcessHandle.current().pid());
    log.info("Running metadata-extractor one time");
    Boolean isSucceeded =
        tableDiscoveryService
            .discoverTables()
            .thenCompose(tableMetadataUploaderService::uploadInstantsInTables)
            .join();
    if (Boolean.TRUE.equals(isSucceeded)) {
      log.info("Run Completed");
    } else {
      log.error("Run failed");
    }
  }

  private void discoverTables() {
    log.info("Discovering tables in provided paths");
    tableDiscoveryService
        .discoverTables()
        .thenAccept(
            tables -> {
              synchronized (lock) {
                tablesToProcess = tables;
              }
            })
        .exceptionally(
            ex -> {
              log.error("Error discovering tables: ", ex);
              return null;
            })
        .join();
  }

  private void processTables() {
    log.debug("Polling to see if metadata needs to be uploaded");
    Instant tableMetadataUploadRunStartTime = Instant.now();
    if (Duration.between(previousTableMetadataUploadRunStartTime, tableMetadataUploadRunStartTime)
            .toMinutes()
        >= TABLE_METADATA_UPLOAD_INTERVAL_MINUTES) {
      Set<Table> tables = null;
      synchronized (lock) {
        if (tablesToProcess != null) {
          tables = new HashSet<>(tablesToProcess);
        }
      }
      if (tables != null && !tables.isEmpty()) {
        log.debug("Uploading table metadata for discovered tables");
        tableMetadataUploaderService
            .uploadInstantsInTables(tables)
            .exceptionally(
                ex -> {
                  log.error("Error uploading instants in tables: ", ex);
                  return null;
                })
            .join();
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

  @VisibleForTesting
  int getProcessTableMetadataSyncDurationSeconds() {
    return PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;
  }
}
