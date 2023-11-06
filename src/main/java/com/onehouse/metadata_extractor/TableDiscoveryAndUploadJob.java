package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.onehouse.metadata_extractor.models.Table;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class TableDiscoveryAndUploadJob {
  private final TableDiscoveryService tableDiscoveryService;
  private final TableMetadataUploaderService tableMetadataUploaderService;
  private final ScheduledExecutorService scheduler;
  private final Object lock = new Object();
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDiscoveryAndUploadJob.class);
  private Set<Table> tablesToProcess;

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
    scheduler.scheduleWithFixedDelay(
        this::processTables, 0, TABLE_METADATA_UPLOAD_INTERVAL_MINUTES, TimeUnit.MINUTES);
  }

  /*
   * Runs table discovery followed by metadata uploader once
   */
  public void runOnce() {
    log.debug("Running metadata-extractor one time");
    tableDiscoveryService
        .discoverTables()
        .thenCompose(tableMetadataUploaderService::uploadInstantsInTables)
        .join();
  }

  private void discoverTables() {
    log.debug("Discovering tables in provided paths");
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
              LOGGER.error("Error discovering tables: " + ex.getMessage());
              return null;
            });
  }

  private void processTables() {
    log.debug("Uploading table metadata for discovered tables");
    Set<Table> tables;
    synchronized (lock) {
      tables = tablesToProcess;
    }
    if (tables != null && !tables.isEmpty()) {
      tableMetadataUploaderService
          .uploadInstantsInTables(tables)
          .exceptionally(
              ex -> {
                LOGGER.error("Error uploading instants in tables: " + ex.getMessage());
                return null;
              });
    }
  }

  public void shutdown() {
    scheduler.shutdown();
  }

  @VisibleForTesting
  ScheduledExecutorService getScheduler(){
    return Executors.newScheduledThreadPool(2);
  }
}
