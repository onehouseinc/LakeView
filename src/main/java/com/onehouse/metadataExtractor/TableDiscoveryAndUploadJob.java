package com.onehouse.metadataExtractor;

import static com.onehouse.metadataExtractor.Constants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static com.onehouse.metadataExtractor.Constants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;

import com.google.inject.Inject;
import com.onehouse.metadataExtractor.models.Table;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.tableDiscoveryService = tableDiscoveryService;
    this.tableMetadataUploaderService = tableMetadataUploaderService;
  }

  public void start() {
    // Schedule table discovery
    scheduler.scheduleAtFixedRate(
        this::discoverTables, 0, TABLE_DISCOVERY_INTERVAL_MINUTES, TimeUnit.MINUTES);

    // Schedule table processing
    scheduler.scheduleWithFixedDelay(
        this::processTables, 0, TABLE_METADATA_UPLOAD_INTERVAL_MINUTES, TimeUnit.MINUTES);
  }

  private void discoverTables() {
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
}
