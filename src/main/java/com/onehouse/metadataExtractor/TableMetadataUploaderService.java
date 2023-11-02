package com.onehouse.metadataExtractor;

import static com.onehouse.metadataExtractor.Constants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.metadataExtractor.Constants.HOODIE_FOLDER_NAME;
import static com.onehouse.metadataExtractor.Constants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.metadataExtractor.Constants.INITIAL_CHECKPOINT;
import static com.onehouse.metadataExtractor.Constants.PRESIGNED_URL_REQUEST_BATCH_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.request.CommitTimelineType;
import com.onehouse.api.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.metadataExtractor.models.Checkpoint;
import com.onehouse.metadataExtractor.models.Table;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMetadataUploaderService {
  private final AsyncStorageClient asyncStorageClient;
  private final HoodiePropertiesReader hoodiePropertiesReader;
  private final PresignedUrlFileUploader presignedUrlFileUploader;
  private final StorageUtils storageUtils;
  private final OnehouseApiClient onehouseApiClient;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataUploaderService.class);

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
  }

  public CompletableFuture<Void> processTables(Set<Table> tablesToProcess) {
    logger.debug("Uploading metadata of following tables: " + tablesToProcess);
    List<CompletableFuture<Void>> processTablesFuture = new ArrayList<>();
    for (Table table : tablesToProcess) {
      processTablesFuture.add(processTable(table));
    }

    return CompletableFuture.allOf(processTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(ignored -> null);
  }

  private CompletableFuture<Void> processTable(Table table) {
    UUID tableId = getTableIdFromAbsolutePathUrl(table.getAbsoluteTableUrl());
    logger.debug("Fetching checkpoint for table: " + table);
    return onehouseApiClient
        .getTableMetricsCheckpoint(tableId.toString())
        .thenCompose(
            getTableMetricsCheckpointResponse -> {
              // TODO: verify that this is the right status code
              if (getTableMetricsCheckpointResponse.isFailure()
                  && getTableMetricsCheckpointResponse.getStatusCode() == 404) {
                // checkpoint not found, table needs to be registered
                logger.debug("Checkpoint not found, processing table for the first time: " + table);
                return hoodiePropertiesReader
                    .readHoodieProperties(getHoodiePropertiesFilePath(table))
                    .thenCompose(
                        properties ->
                            onehouseApiClient.initializeTableMetricsCheckpoint(
                                InitializeTableMetricsCheckpointRequest.builder()
                                    .tableId(tableId)
                                    .tableName(properties.getTableName())
                                    .tableType(properties.getTableType())
                                    .tableBasePath(table.getRelativeTablePath())
                                    .databaseName(table.getDatabaseName())
                                    .lakeName(table.getLakeName())
                                    .build()))
                    .thenCompose(
                        initializeTableMetricsCheckpointResponse -> {
                          if (!initializeTableMetricsCheckpointResponse.isFailure()) {
                            return processInstantsInTable(tableId, table, INITIAL_CHECKPOINT);
                          }
                          throw new RuntimeException(
                              "Failed to initialise table for processing, " + table);
                        });
              }
              try {
                // process from previous checkpoint
                return processInstantsInTable(
                    tableId,
                    table,
                    mapper.readValue(
                        getTableMetricsCheckpointResponse.getCheckpoint(), Checkpoint.class));
              } catch (JsonProcessingException e) {
                throw new RuntimeException("Error deserializing checkpoint value", e);
              }
            });
  }

  private CompletableFuture<Void> processInstantsInTable(
      UUID tableId, Table table, Checkpoint checkpoint) {
    if (!checkpoint.getIsArchivedCommitsProcessed()) {
      return processInstantsInTimeline(
              tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
          .thenCompose(
              ignore ->
                  processInstantsInTimeline(
                      tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE));
    }
    return processInstantsInTimeline(
        tableId, table, checkpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  private CompletableFuture<Void> processInstantsInTimeline(
      UUID tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    String pathSuffix = getPathSuffix(commitTimelineType);

    String directoryUrl = storageUtils.constructFilePath(table.getAbsoluteTableUrl(), pathSuffix);
    return asyncStorageClient
        .listFiles(directoryUrl)
        .thenCompose(
            filesList -> {
              List<File> filesToProcess = getFilesToProcess(filesList, checkpoint);

              List<List<File>> batches =
                  Lists.partition(filesToProcess, PRESIGNED_URL_REQUEST_BATCH_SIZE);
              int numBatches = batches.size();

              List<CompletableFuture<Void>> futures =
                  IntStream.range(0, batches.size())
                      .mapToObj(
                          batchIndex ->
                              CompletableFuture.runAsync(
                                  () -> {
                                    List<File> batch = batches.get(batchIndex);
                                    File lastUploadedFile = batch.get(batch.size() - 1);
                                    processBatch(tableId, batch, commitTimelineType, directoryUrl)
                                        .thenCompose(
                                            ignore ->
                                                updateCheckpointAfterProcessingBatch(
                                                    tableId,
                                                    checkpoint,
                                                    numBatches,
                                                    batchIndex,
                                                    lastUploadedFile,
                                                    batch.stream()
                                                        .map(File::getFilename)
                                                        .collect(Collectors.toList()),
                                                    commitTimelineType));
                                  },
                                  executorService))
                      .collect(Collectors.toList());

              return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            });
  }

  private CompletableFuture<Void> processBatch(
      UUID tableId, List<File> batch, CommitTimelineType commitTimelineType, String directoryUrl) {
    return onehouseApiClient
        .generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(tableId)
                .commitInstants(batch.stream().map(File::getFilename).collect(Collectors.toList()))
                .commitTimelineType(commitTimelineType)
                .build())
        .thenCompose(
            generateCommitMetadataUploadUrlResponse -> {
              if (generateCommitMetadataUploadUrlResponse.isFailure()) {
                throw new RuntimeException(
                    "failed to generate presigned urls: "
                        + generateCommitMetadataUploadUrlResponse.getCause());
              }

              List<CompletableFuture<Void>> uploadFutures = new ArrayList<>();
              for (int i = 0; i < batch.size(); i++) {
                uploadFutures.add(
                    presignedUrlFileUploader.uploadFileToPresignedUrl(
                        generateCommitMetadataUploadUrlResponse.getUploadUrls().get(i),
                        storageUtils.constructFilePath(directoryUrl, batch.get(i).getFilename())));
              }

              return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]));
            });
  }

  private CompletableFuture<Void> updateCheckpointAfterProcessingBatch(
      UUID tableId,
      Checkpoint PreviousCheckpoint,
      int numBatches,
      int batchIndex,
      File lastUploadedFile,
      List<String> filesUploaded,
      CommitTimelineType commitTimelineType) {

    Checkpoint updatedCheckpoint =
        Checkpoint.builder()
            .batchId(PreviousCheckpoint.getBatchId() + batchIndex + 1)
            .lastUploadedFile(lastUploadedFile.getFilename())
            .checkpoint(lastUploadedFile.getLastModifiedAt())
            .isArchivedCommitsProcessed(
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType))
            .isArchivedCommitsProcessed(batchIndex >= numBatches - 1) // TODO: handle case where
            // new archived commit is added during upload
            .build();
    try {
      return onehouseApiClient
          .upsertTableMetricsCheckpoint(
              UpsertTableMetricsCheckpointRequest.builder()
                  .commitTimelineType(commitTimelineType)
                  .tableId(tableId)
                  .Checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                  .filesUploaded(filesUploaded)
                  .build())
          .thenCompose(
              upsertTableMetricsCheckpointResponse -> {
                if (upsertTableMetricsCheckpointResponse.isFailure()) {
                  throw new RuntimeException(
                      "failed to update PreviousCheckpoint: "
                          + upsertTableMetricsCheckpointResponse.getCause());
                }
                return null;
              });
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String getPathSuffix(CommitTimelineType commitTimelineType) {
    String pathSuffix = HOODIE_FOLDER_NAME + "/";
    return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        ? pathSuffix + ARCHIVED_FOLDER_NAME + '/'
        : pathSuffix;
  }

  private List<File> getFilesToProcess(List<File> filesList, Checkpoint checkpoint) {
    List<File> filteredAndSortedFiles =
        filesList.stream()
            .filter(file -> !file.getIsDirectory()) // filter out directories
            .filter( // hoodie properties file is uploaded only once
                file -> !file.getFilename().startsWith(HOODIE_PROPERTIES_FILE))
            .filter(file -> !file.getLastModifiedAt().isBefore(checkpoint.getCheckpoint()))
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

    List<File> filesToProcess =
        lastUploadedIndexOpt.isPresent()
            ? filteredAndSortedFiles.subList(
                lastUploadedIndexOpt.getAsInt() + 1, filteredAndSortedFiles.size())
            : filteredAndSortedFiles;
    if (checkpoint.getBatchId() == 0) {
      File HudiPropertiesFile =
          File.builder().filename(HOODIE_PROPERTIES_FILE).isDirectory(false).build();
      filesToProcess.add(0, HudiPropertiesFile);
    }

    return filesToProcess;
  }

  private String getHoodiePropertiesFilePath(Table table) {
    String basePath = table.getAbsoluteTableUrl();
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
