package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_COMMIT_INSTANT_PATTERN;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_CHECKPOINT;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_PROCESSING_BATCH_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.Table;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  public CompletableFuture<Boolean> uploadInstantsInTables(Set<Table> tablesToProcess) {
    log.info("Uploading metadata of following tables: " + tablesToProcess);
    List<Table> tableWithIds =
        tablesToProcess.stream().map(this::updateTableIdIfNotPresent).collect(Collectors.toList());
    List<List<Table>> tableBatches =
        Lists.partition(new ArrayList<>(tableWithIds), TABLE_PROCESSING_BATCH_SIZE);

    CompletableFuture<Boolean> processTableBatchFuture = CompletableFuture.completedFuture(true);

    // process batches one after another
    for (List<Table> tableBatch : tableBatches) {
      processTableBatchFuture =
          processTableBatchFuture.thenComposeAsync(
              previousResult ->
                  uploadInstantsInTableBatch(tableBatch)
                      .thenApply(currentResult -> previousResult && currentResult),
              executorService);
    }

    return processTableBatchFuture;
  }

  private Table updateTableIdIfNotPresent(Table table) {
    if (StringUtils.isNotBlank(table.getTableId())) {
      return table;
    }
    return table
        .toBuilder()
        .tableId(getTableIdFromAbsolutePathUrl(table.getAbsoluteTableUri()).toString())
        .build();
  }

  private CompletableFuture<Boolean> uploadInstantsInTableBatch(List<Table> tables) {
    log.info("Fetching checkpoint for tables: " + tables);
    return onehouseApiClient
        .getTableMetricsCheckpoints(
            tables.stream().map(Table::getTableId).collect(Collectors.toList()))
        .thenComposeAsync(
            getTableMetricsCheckpointResponse -> {
              if (getTableMetricsCheckpointResponse.isFailure()) {
                log.error(
                    "Error encountered when fetching checkpoint, skipping table processing.status code: {} message {}",
                    getTableMetricsCheckpointResponse.getStatusCode(),
                    getTableMetricsCheckpointResponse.getCause());
                return CompletableFuture.completedFuture(false);
              }

              Set<String> tableIdsWithCheckpoint =
                  getTableMetricsCheckpointResponse.getCheckpoints().stream()
                      .map(GetTableMetricsCheckpointResponse.TableMetadataCheckpoint::getTableId)
                      .collect(Collectors.toSet());
              List<Table> tablesToInitialise =
                  tables.stream()
                      .filter(table -> !tableIdsWithCheckpoint.contains(table.getTableId()))
                      .collect(Collectors.toList());

              List<CompletableFuture<Boolean>> processTablesFuture = new ArrayList<>();

              Map<String, GetTableMetricsCheckpointResponse.TableMetadataCheckpoint>
                  tableCheckpointMap =
                      getTableMetricsCheckpointResponse.getCheckpoints().stream()
                          .collect(
                              Collectors.toMap(
                                  GetTableMetricsCheckpointResponse.TableMetadataCheckpoint
                                      ::getTableId,
                                  Function.identity()));

              for (Table table : tables) {
                if (tableCheckpointMap.containsKey(table.getTableId())) {
                  try {
                    // checkpoints found, continue from previous checkpoint
                    String checkpointString =
                        tableCheckpointMap.get(table.getTableId()).getCheckpoint();
                    processTablesFuture.add(
                        uploadNewInstantsSinceCheckpoint(
                            table.getTableId(),
                            table,
                            StringUtils.isNotBlank(checkpointString)
                                ? mapper.readValue(checkpointString, Checkpoint.class)
                                : INITIAL_CHECKPOINT));
                  } catch (JsonProcessingException e) {
                    log.error(
                        "Error deserializing checkpoint value for table: {}, skipping table",
                        table,
                        e);
                  }
                }
              }

              CompletableFuture<List<CompletableFuture<Boolean>>>
                  initialiseAndProcessNewlyDiscoveredTablesFuture =
                      initialiseAndProcessNewlyDiscoveredTables(tablesToInitialise);

              return initialiseAndProcessNewlyDiscoveredTablesFuture.thenComposeAsync(
                  discoveredTablesProcessingFuture -> {
                    processTablesFuture.addAll(discoveredTablesProcessingFuture);
                    return CompletableFuture.allOf(
                            processTablesFuture.toArray(new CompletableFuture[0]))
                        .thenApply(
                            ignored ->
                                processTablesFuture.stream()
                                    .map(CompletableFuture::join)
                                    .allMatch(Boolean.TRUE::equals));
                  }, // return false if processing any table failed
                  executorService);
            },
            executorService)
        .exceptionally(
            throwable -> {
              log.error("Encountered exception when uploading instants", throwable);
              return false;
            });
  }

  private CompletableFuture<List<CompletableFuture<Boolean>>>
      initialiseAndProcessNewlyDiscoveredTables(List<Table> tablesToInitialise) {
    List<CompletableFuture<Boolean>> processTablesFuture = new ArrayList<>();
    CompletableFuture<List<CompletableFuture<Boolean>>>
        initialiseAndProcessNewlyDiscoveredTablesFuture =
            CompletableFuture.completedFuture(
                Collections.singletonList(CompletableFuture.completedFuture(true)));
    if (!tablesToInitialise.isEmpty()) {
      log.info("Initializing following tables {}", tablesToInitialise);
      List<
              CompletableFuture<
                  InitializeTableMetricsCheckpointRequest
                      .InitializeSingleTableMetricsCheckpointRequest>>
          initializeSingleTableMetricsCheckpointRequestFutureList = new ArrayList<>();
      for (Table table : tablesToInitialise) {
        initializeSingleTableMetricsCheckpointRequestFutureList.add(
            hoodiePropertiesReader
                .readHoodieProperties(getHoodiePropertiesFilePath(table))
                .thenApply(
                    properties -> {
                      if (properties == null) {
                        log.error(
                            "Encountered exception when reading hoodie.properties file for table: {}, skipping this table",
                            table);
                        return null; // will be filtered out later
                      }
                      return InitializeTableMetricsCheckpointRequest
                          .InitializeSingleTableMetricsCheckpointRequest.builder()
                          .tableId(table.getTableId())
                          .tableName(properties.getTableName())
                          .tableType(properties.getTableType())
                          .databaseName(table.getDatabaseName())
                          .lakeName(table.getLakeName())
                          .tableBasePath(table.getAbsoluteTableUri())
                          .build();
                    }));
      }
      initialiseAndProcessNewlyDiscoveredTablesFuture =
          CompletableFuture.allOf(
                  initializeSingleTableMetricsCheckpointRequestFutureList.toArray(
                      new CompletableFuture[0]))
              .thenComposeAsync(
                  ignored -> {
                    List<
                            InitializeTableMetricsCheckpointRequest
                                .InitializeSingleTableMetricsCheckpointRequest>
                        initializeSingleTableMetricsCheckpointRequestList =
                            initializeSingleTableMetricsCheckpointRequestFutureList.stream()
                                .map(CompletableFuture::join)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                    return onehouseApiClient.initializeTableMetricsCheckpoint(
                        InitializeTableMetricsCheckpointRequest.builder()
                            .tables(initializeSingleTableMetricsCheckpointRequestList)
                            .build());
                  },
                  executorService)
              .thenComposeAsync(
                  initializeTableMetricsCheckpointResponse -> {
                    if (initializeTableMetricsCheckpointResponse.isFailure()) {
                      log.error(
                          "Error encountered when initialising tables, skipping table processing.status code: {} message {}",
                          initializeTableMetricsCheckpointResponse.getStatusCode(),
                          initializeTableMetricsCheckpointResponse.getCause());
                      return CompletableFuture.completedFuture(
                          List.of(CompletableFuture.completedFuture(false)));
                    }

                    Map<
                            String,
                            InitializeTableMetricsCheckpointResponse
                                .InitializeSingleTableMetricsCheckpointResponse>
                        initialiseTableMetricsCheckpointMap =
                            initializeTableMetricsCheckpointResponse.getResponse().stream()
                                .collect(
                                    Collectors.toMap(
                                        InitializeTableMetricsCheckpointResponse
                                                .InitializeSingleTableMetricsCheckpointResponse
                                            ::getTableId,
                                        Function.identity(),
                                        (oldValue, newValue) -> {
                                          log.warn(
                                              "duplicate found! old value: {} new value: {}",
                                              oldValue,
                                              newValue);
                                          return newValue;
                                        }));
                    for (Table table : tablesToInitialise) {
                      InitializeTableMetricsCheckpointResponse
                              .InitializeSingleTableMetricsCheckpointResponse
                          response =
                              initialiseTableMetricsCheckpointMap.getOrDefault(
                                  table.getTableId(), null);
                      if (response == null) {
                        // table not initialised due to errors in previous steps
                        continue;
                      }
                      if (!StringUtils.isBlank(response.getError())) {
                        log.error(
                            "Error initialising table: {} error: {}, skipping table processing",
                            table,
                            response.getError());
                        continue;
                      }
                      processTablesFuture.add(
                          uploadNewInstantsSinceCheckpoint(
                              table.getTableId(), table, INITIAL_CHECKPOINT));
                    }
                    return CompletableFuture.completedFuture(processTablesFuture);
                  },
                  executorService);
    }
    return initialiseAndProcessNewlyDiscoveredTablesFuture;
  }

  private CompletableFuture<Boolean> uploadNewInstantsSinceCheckpoint(
      String tableId, Table table, Checkpoint checkpoint) {
    if (!checkpoint.isArchivedCommitsProcessed()) {
      /*
       * if archived commits are not uploaded, we upload those first before moving to active timeline
       * commits in archived timeline are uploaded only once.
       */
      return timelineCommitInstantsUploader
          .batchUploadWithCheckpoint(
              tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
          .thenComposeAsync(
              archivedTimelineCheckpoint -> {
                if (archivedTimelineCheckpoint == null) {
                  // do not upload instants in active timeline if there was failure
                  log.warn(
                      "Skipping uploading instants in active timeline due to failures in uploading archived timeline instants for table {}",
                      table.getAbsoluteTableUri());
                  return CompletableFuture.completedFuture(false);
                }
                return timelineCommitInstantsUploader
                    .paginatedBatchUploadWithCheckpoint(
                        tableId,
                        table,
                        resetCheckpoint(archivedTimelineCheckpoint),
                        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                    .thenApply(Objects::nonNull);
              },
              executorService);
    }

    /*
     * if the last processed file in the retrieved checkpoint is an archived-commit,
     * then we reset the checkpoint timestamp and continuation token
     * else we use the retrieved checkpoint.
     * this allows us to continue from the previous batch id
     */
    Checkpoint activeTimelineCheckpoint =
        ARCHIVED_COMMIT_INSTANT_PATTERN.matcher(checkpoint.getLastUploadedFile()).matches()
            ? resetCheckpoint(checkpoint)
            : checkpoint;
    return timelineCommitInstantsUploader
        .paginatedBatchUploadWithCheckpoint(
            tableId,
            table,
            activeTimelineCheckpoint,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
        .thenApply(Objects::nonNull);
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

  private static Checkpoint resetCheckpoint(Checkpoint checkpoint) {
    return checkpoint.toBuilder().checkpointTimestamp(Instant.EPOCH).lastUploadedFile("").build();
  }
}
