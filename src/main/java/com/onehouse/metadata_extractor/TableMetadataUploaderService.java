package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ACTIVE_TIMELINE_CHECKPOINT;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ARCHIVED_TIMELINE_CHECKPOINT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/*
 * Uploads Instants in the active and archived timeline for the tables which were discovered
 */
@Slf4j
public class TableMetadataUploaderService {
  private final HoodiePropertiesReader hoodiePropertiesReader;
  private final OnehouseApiClient onehouseApiClient;
  private final TimelineCommitInstantsUploader timelineCommitInstantsUploader;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;
  private static final Pattern ARCHIVED_TIMELINE_COMMIT_INSTANT_PATTERN =
      Pattern.compile("\\.commits_\\.archive\\.\\d+_\\d+-\\d+-\\d+");

  @Inject
  public TableMetadataUploaderService(
      @Nonnull HoodiePropertiesReader hoodiePropertiesReader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull TimelineCommitInstantsUploader timelineCommitInstantsUploader,
      @Nonnull ExecutorService executorService) {
    this.hoodiePropertiesReader = hoodiePropertiesReader;
    this.onehouseApiClient = onehouseApiClient;
    this.timelineCommitInstantsUploader = timelineCommitInstantsUploader;
    this.executorService = executorService;
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
  }

  public CompletableFuture<Void> uploadInstantsInTables(Set<Table> tablesToProcess) {
    log.info("Uploading metadata of following tables: " + tablesToProcess);
    List<CompletableFuture<Boolean>> processTablesFuture = new ArrayList<>();
    for (Table table : tablesToProcess) {
      processTablesFuture.add(uploadInstantsInTable(table));
    }

    return CompletableFuture.allOf(processTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(ignored -> null);
  }

  private CompletableFuture<Boolean> uploadInstantsInTable(Table table) {
    log.debug("Fetching checkpoint for table: " + table);
    UUID tableId = getTableIdFromAbsolutePathUrl(table.getAbsoluteTableUri());
    return onehouseApiClient
        .getTableMetricsCheckpoint(tableId.toString())
        .thenComposeAsync(
            getTableMetricsCheckpointResponse -> {
              if (getTableMetricsCheckpointResponse.isFailure()) {
                if (getTableMetricsCheckpointResponse.getStatusCode() != 404) {
                  log.error(
                      "Error encountered when fetching checkpoint, skipping table processing. {}",
                      getTableMetricsCheckpointResponse.getCause());
                  return CompletableFuture.completedFuture(false);
                } else {
                  // checkpoint not found, table needs to be registered
                  log.info("Initializing table {}", table.getAbsoluteTableUri());
                  return hoodiePropertiesReader
                      .readHoodieProperties(getHoodiePropertiesFilePath(table))
                      .thenCompose(
                          properties ->
                              onehouseApiClient.initializeTableMetricsCheckpoint(
                                  InitializeTableMetricsCheckpointRequest.builder()
                                      .tableId(tableId)
                                      .tableName(properties.getTableName())
                                      .tableType(properties.getTableType())
                                      .tableBasePath(
                                          table.getRelativeTablePath()) // sending relative instead
                                      // of
                                      // absolute path to avoid sending sensitive data
                                      .databaseName(table.getDatabaseName())
                                      .lakeName(table.getLakeName())
                                      .build()))
                      .thenCompose(
                          initializeTableMetricsCheckpointResponse -> {
                            if (!initializeTableMetricsCheckpointResponse.isFailure()) {
                              return uploadNewInstantsSinceCheckpoint(
                                  tableId, table, INITIAL_ARCHIVED_TIMELINE_CHECKPOINT);
                            }
                            log.error(
                                "Failed to initialise table for processing, Exception: {} , Table: {}. skipping table",
                                initializeTableMetricsCheckpointResponse.getCause(),
                                table);
                            // skip uploading instants for this table in the current run
                            return null;
                          })
                      .exceptionally(
                          throwable -> {
                            log.error(
                                "error processing table: {}",
                                table.getAbsoluteTableUri(),
                                throwable);
                            return null;
                          });
                }
              }
              try {
                // process from previous checkpoint
                String checkpointString = getTableMetricsCheckpointResponse.getCheckpoint();
                return uploadNewInstantsSinceCheckpoint(
                    tableId,
                    table,
                    StringUtils.isNotBlank(checkpointString)
                        ? mapper.readValue(checkpointString, Checkpoint.class)
                        : INITIAL_ARCHIVED_TIMELINE_CHECKPOINT);
              } catch (JsonProcessingException e) {
                throw new RuntimeException("Error deserializing checkpoint value", e);
              }
            },
            executorService);
  }

  private CompletableFuture<Boolean> uploadNewInstantsSinceCheckpoint(
      UUID tableId, Table table, Checkpoint checkpoint) {
    if (!checkpoint.isArchivedCommitsProcessed()) {
      /*
       * if archived commits are not uploaded, we upload those first before moving to active timeline
       * commits in archived timeline are uploaded only once, when the table is registered for the first time.
       */
      return timelineCommitInstantsUploader
          .uploadInstantsInTimelineSinceCheckpoint(
              tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
          .thenComposeAsync(
              uploadInstantsInArchivedTimelineSucceeded -> {
                if (!Boolean.TRUE.equals(uploadInstantsInArchivedTimelineSucceeded)) {
                  // do not upload instants in active timeline if there was failure
                  log.warn(
                      "Skipping uploading instants in active timeline due to failures in uploading archived timeline instants for table {}",
                      table.getAbsoluteTableUri());
                  return CompletableFuture.completedFuture(false);
                }
                // batch_id starts afresh as we are now processing the active-timeline
                return timelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
                    tableId,
                    table,
                    INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
                    CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
              },
              executorService);
    }

    /*
     * if the last processed file in the retrieved checkpoint is an archived-commit, then we reset the checkpoint
     * else we use the retrieved checkpoint.
     * this allows us to maintain separate batch_id's for the commits in the two timelines
     */
    Checkpoint activeTimelineCheckpoint =
        ARCHIVED_TIMELINE_COMMIT_INSTANT_PATTERN.matcher(checkpoint.getLastUploadedFile()).matches()
            ? INITIAL_ACTIVE_TIMELINE_CHECKPOINT
            : checkpoint;
    return timelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
        tableId, table, activeTimelineCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  private String getHoodiePropertiesFilePath(Table table) {
    String basePath = table.getAbsoluteTableUri();
    return String.format(
        "%s/%s/%s",
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath,
        HOODIE_FOLDER_NAME,
        HOODIE_PROPERTIES_FILE);
  }

  private static UUID getTableIdFromAbsolutePathUrl(String tableAbsolutePathUrl) {
    return UUID.nameUUIDFromBytes(tableAbsolutePathUrl.getBytes());
  }
}
