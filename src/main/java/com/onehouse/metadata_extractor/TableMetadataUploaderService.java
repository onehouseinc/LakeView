package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_CHECKPOINT;
import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.PresignedUrlFileUploader;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/*
 * Uploads Instants in the active and archived timeline for the tables which were discovered
 */
@Slf4j
public class TableMetadataUploaderService {
  private final AsyncStorageClient asyncStorageClient;
  private final HoodiePropertiesReader hoodiePropertiesReader;
  private final PresignedUrlFileUploader presignedUrlFileUploader;
  private final StorageUtils storageUtils;
  private final OnehouseApiClient onehouseApiClient;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;

  @Inject
  public TableMetadataUploaderService(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull HoodiePropertiesReader hoodiePropertiesReader,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull StorageUtils storageUtils,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageClient = asyncStorageClient;
    this.hoodiePropertiesReader = hoodiePropertiesReader;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.storageUtils = storageUtils;
    this.onehouseApiClient = onehouseApiClient;
    this.executorService = executorService;
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
  }

  public CompletableFuture<Void> uploadInstantsInTables(Set<Table> tablesToProcess) {
    log.debug("Uploading metadata of following tables: " + tablesToProcess);
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
        .thenCompose(
            getTableMetricsCheckpointResponse -> {
              // TODO: @Sampan verify that this is the right status code during E2E
              if (getTableMetricsCheckpointResponse.isFailure()
                  && getTableMetricsCheckpointResponse.getStatusCode() == 404) {
                // checkpoint not found, table needs to be registered
                log.debug("Checkpoint not found, processing table for the first time: " + table);
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
                                        table.getRelativeTablePath()) // sending relative instead of
                                    // absolute path to avoid
                                    // sending sensitive data
                                    .databaseName(table.getDatabaseName())
                                    .lakeName(table.getLakeName())
                                    .build()))
                    .thenCompose(
                        initializeTableMetricsCheckpointResponse -> {
                          if (!initializeTableMetricsCheckpointResponse.isFailure()) {
                            return uploadNewInstantsSinceCheckpoint(
                                tableId, table, INITIAL_CHECKPOINT);
                          }
                          log.error(
                              "Failed to initialise table for processing, Exception: {} , Table: {}. skipping table",
                              initializeTableMetricsCheckpointResponse.getCause(),
                              table);
                          // skip uploading instants for this table in current run
                          return null;
                        })
                    .exceptionally(
                        throwable -> {
                          log.error(
                              "error processing table: {}", table.getAbsoluteTableUri(), throwable);
                          return null;
                        });
              }
              try {
                // process from previous checkpoint
                return uploadNewInstantsSinceCheckpoint(
                    tableId,
                    table,
                    mapper.readValue(
                        getTableMetricsCheckpointResponse.getCheckpoint(), Checkpoint.class));
              } catch (JsonProcessingException e) {
                throw new RuntimeException("Error deserializing checkpoint value", e);
              }
            });
  }

  private CompletableFuture<Boolean> uploadNewInstantsSinceCheckpoint(
      UUID tableId, Table table, Checkpoint checkpoint) {
    if (!checkpoint.isArchivedCommitsProcessed()) {
      // if archived commits are not uploaded, we upload those first before moving to active
      // timeline
      return uploadInstantsInTimeline(
              tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
          .thenCompose(
              uploadInstantsInArchivedTimelineSucceeded -> {
                if (!Boolean.TRUE.equals(uploadInstantsInArchivedTimelineSucceeded)) {
                  // do not upload instants in active timeline if there was failure
                  log.warn(
                      "Skipping uploading instants in active timeline due to failures in uploading archived timeline instants for table {}",
                      table.getAbsoluteTableUri());
                  return CompletableFuture.completedFuture(false);
                }
                return uploadInstantsInTimeline(
                    tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
              });
    }
    // commits in archived timeline are uploaded only once, when the table is registered for the
    // first time.
    return uploadInstantsInTimeline(
        tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  private CompletableFuture<Boolean> uploadInstantsInTimeline(
      UUID tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    String pathSuffix = getPathSuffix(commitTimelineType);

    String directoryUrl = storageUtils.constructFileUri(table.getAbsoluteTableUri(), pathSuffix);
    return asyncStorageClient
        .listAllFilesInDir(directoryUrl)
        .thenCompose(
            filesList -> {
              List<File> filesToProcess =
                  getFilesToUploadBasedOnPreviousCheckpoint(filesList, checkpoint);

              List<List<File>> batches =
                  Lists.partition(filesToProcess, PRESIGNED_URL_REQUEST_BATCH_SIZE);
              int numBatches = batches.size();

              // processing batches while maintaining the sequential order
              CompletableFuture<Boolean> sequentialBatchProcessingFuture =
                  CompletableFuture.completedFuture(true);
              int batchIndex = 0;
              for (List<File> batch : batches) {
                int finalBatchIndex = batchIndex;
                sequentialBatchProcessingFuture =
                    sequentialBatchProcessingFuture.thenComposeAsync(
                        previousBatchProcessingSucceeded -> {
                          if (!Boolean.TRUE.equals(previousBatchProcessingSucceeded)) {
                            // don't process any further batches
                            return CompletableFuture.completedFuture(false);
                          }
                          File lastUploadedFile = batch.get(batch.size() - 1);
                          return uploadBatch(tableId, batch, commitTimelineType, directoryUrl)
                              .thenComposeAsync(
                                  ignored2 ->
                                      // update checkpoint after uploading each batch for quick
                                      // recovery in case of failures
                                      updateCheckpointAfterProcessingBatch(
                                          tableId,
                                          checkpoint,
                                          numBatches,
                                          finalBatchIndex,
                                          lastUploadedFile,
                                          batch.stream()
                                              .map(File::getFilename)
                                              .collect(Collectors.toList()),
                                          commitTimelineType),
                                  executorService)
                              .exceptionally(
                                  throwable -> {
                                    // will catch any failures when uploading batch / updating
                                    // checkpoint
                                    log.error(
                                        "error processing batch for table: {}. Skipping processing of further batches of table in current run.",
                                        table.getAbsoluteTableUri(),
                                        throwable);
                                    return false;
                                  });
                        },
                        executorService);
                batchIndex += 1;
              }

              return sequentialBatchProcessingFuture;
            });
  }

  private CompletableFuture<Void> uploadBatch(
      UUID tableId, List<File> batch, CommitTimelineType commitTimelineType, String directoryUrl) {
    return onehouseApiClient
        .generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(tableId.toString())
                .commitInstants(batch.stream().map(File::getFilename).collect(Collectors.toList()))
                .commitTimelineType(commitTimelineType)
                .build())
        .thenCompose(
            generateCommitMetadataUploadUrlResponse -> {
              if (generateCommitMetadataUploadUrlResponse.isFailure()) {
                throw new RuntimeException(
                    String.format(
                        "failed to generate presigned urls: %s",
                        generateCommitMetadataUploadUrlResponse.getCause()));
              }

              List<CompletableFuture<Void>> uploadFutures = new ArrayList<>();
              for (int i = 0; i < batch.size(); i++) {
                uploadFutures.add(
                    presignedUrlFileUploader.uploadFileToPresignedUrl(
                        generateCommitMetadataUploadUrlResponse.getUploadUrls().get(i),
                        storageUtils.constructFileUri(directoryUrl, batch.get(i).getFilename())));
              }

              return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]));
            });
  }

  private CompletableFuture<Boolean> updateCheckpointAfterProcessingBatch(
      UUID tableId,
      Checkpoint previousCheckpoint,
      int numBatches,
      int batchIndex,
      File lastUploadedFile,
      List<String> filesUploaded,
      CommitTimelineType commitTimelineType) {

    boolean archivedCommitsProcessed =
        true; // archived instants would be processed if timeline type is active
    int batchId = previousCheckpoint.getBatchId() + batchIndex + 1;
    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)) {
      archivedCommitsProcessed = (batchIndex >= numBatches - 1);
      if (archivedCommitsProcessed) {
        batchId = 0; // reset batch id when processing active timeline
      }
    }

    Checkpoint updatedCheckpoint =
        Checkpoint.builder()
            .batchId(batchId)
            .lastUploadedFile(lastUploadedFile.getFilename())
            .checkpointTimestamp(lastUploadedFile.getLastModifiedAt())
            .archivedCommitsProcessed(archivedCommitsProcessed)
            .build();
    try {
      return onehouseApiClient
          .upsertTableMetricsCheckpoint(
              UpsertTableMetricsCheckpointRequest.builder()
                  .commitTimelineType(commitTimelineType)
                  .tableId(tableId.toString())
                  .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                  .filesUploaded(filesUploaded)
                  .build())
          .thenApply(
              upsertTableMetricsCheckpointResponse -> {
                if (upsertTableMetricsCheckpointResponse.isFailure()) {
                  throw new RuntimeException(
                      "failed to update PreviousCheckpoint: "
                          + upsertTableMetricsCheckpointResponse.getCause());
                }
                return true;
              });
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new RuntimeException("failed to serialise checkpoint", e));
    }
  }

  private String getPathSuffix(CommitTimelineType commitTimelineType) {
    String pathSuffix = HOODIE_FOLDER_NAME + '/';
    return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        ? pathSuffix + ARCHIVED_FOLDER_NAME + '/'
        : pathSuffix;
  }

  private List<File> getFilesToUploadBasedOnPreviousCheckpoint(
      List<File> filesList, Checkpoint checkpoint) {
    List<File> filteredAndSortedFiles =
        filesList.stream()
            .filter(file -> !file.isDirectory()) // filter out directories
            .filter(file -> !file.getLastModifiedAt().isBefore(checkpoint.getCheckpointTimestamp()))
            .sorted(Comparator.comparing(File::getLastModifiedAt).thenComparing(File::getFilename))
            .collect(Collectors.toList());

    // index of the last file which was uploaded
    OptionalInt lastUploadedIndexOpt =
        IntStream.range(0, filteredAndSortedFiles.size())
            .filter(
                i ->
                    filteredAndSortedFiles
                        .get(i)
                        .getFilename()
                        .equals(checkpoint.getLastUploadedFile()))
            .findFirst();

    return lastUploadedIndexOpt.isPresent()
        ? filteredAndSortedFiles.subList(
            lastUploadedIndexOpt.getAsInt() + 1, filteredAndSortedFiles.size())
        : filteredAndSortedFiles;
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
