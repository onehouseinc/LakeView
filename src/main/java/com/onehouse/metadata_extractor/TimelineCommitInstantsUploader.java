package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE_OBJ;
import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/*
 * Has the core logic for listing and uploading commit instants in a given timeline
 */
@Slf4j
public class TimelineCommitInstantsUploader {
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;
  private final PresignedUrlFileUploader presignedUrlFileUploader;
  private final OnehouseApiClient onehouseApiClient;
  private final ExecutorService executorService;
  private final ObjectMapper mapper;

  @Inject
  TimelineCommitInstantsUploader(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageClient = asyncStorageClient;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
  }

  public CompletableFuture<Checkpoint> uploadInstantsInTimelineSinceCheckpoint(
      UUID tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    String bucketName = storageUtils.getBucketNameFromUri(table.getAbsoluteTableUri());
    String prefix =
        storageUtils.getPathFromUrl(
            storageUtils.constructFileUri(
                table.getAbsoluteTableUri(), getPathSuffixForTimeline(commitTimelineType)));

    return uploadInstantsInTimelineInBatches(
        tableId, table, bucketName, prefix, checkpoint, commitTimelineType, null);
  }

  private CompletableFuture<Checkpoint> uploadInstantsInTimelineInBatches(
      UUID tableId,
      Table table,
      String bucketName,
      String prefix,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType,
      String continuationToken) {
    return asyncStorageClient
        .fetchObjectsByPage(
            bucketName,
            prefix,
            StringUtils.isNotBlank(continuationToken)
                ? continuationToken
                : checkpoint.getContinuationToken())
        .thenComposeAsync(
            continuationTokenAndFiles -> {
              String nextContinuationToken = continuationTokenAndFiles.getLeft();
              List<File> filesToUpload =
                  getFilesToUploadBasedOnPreviousCheckpoint(
                      continuationTokenAndFiles.getRight(), checkpoint);

              // Case 1: We have some files in current page which need to be uploaded
              if (!filesToUpload.isEmpty()) {
                List<List<File>> batches = Lists.partition(filesToUpload, getUploadBatchSize());
                int numBatches = batches.size();

                // processing batches while maintaining the sequential order
                CompletableFuture<Checkpoint> sequentialBatchProcessingFuture =
                    CompletableFuture.completedFuture(checkpoint);
                int batchIndex = 0;
                for (List<File> batch : batches) {
                  boolean processedAllBatchesInCurrentPage = (batchIndex >= numBatches - 1);
                  sequentialBatchProcessingFuture =
                      sequentialBatchProcessingFuture.thenComposeAsync(
                          updatedCheckpoint -> {
                            if (updatedCheckpoint == null) {
                              // don't process any further batches as some error has occurred when
                              // uploading previous batch
                              return CompletableFuture.completedFuture(null);
                            }

                            File lastUploadedFile = batch.get(batch.size() - 1);
                            return uploadBatch(
                                    tableId,
                                    batch,
                                    commitTimelineType,
                                    storageUtils.constructFileUri(
                                        table.getAbsoluteTableUri(),
                                        getPathSuffixForTimeline(commitTimelineType)))
                                .thenComposeAsync(
                                    ignored2 ->
                                        // update checkpoint after uploading each batch for quick
                                        // recovery in case of failures
                                        updateCheckpointAfterProcessingBatch(
                                            tableId,
                                            updatedCheckpoint,
                                            processedAllBatchesInCurrentPage,
                                            lastUploadedFile,
                                            batch.stream()
                                                .map(
                                                    file ->
                                                        getFileNameWithPrefix(
                                                            file, commitTimelineType))
                                                .collect(Collectors.toList()),
                                            commitTimelineType,
                                            checkpoint.getContinuationToken(),
                                            nextContinuationToken),
                                    executorService)
                                .exceptionally(
                                    throwable -> {
                                      // will catch any failures when uploading batch / updating
                                      // checkpoint
                                      log.error(
                                          "error processing batch for table: {}. Skipping processing of further batches of table in current run.",
                                          table.getAbsoluteTableUri(),
                                          throwable);
                                      return null;
                                    });
                          },
                          executorService);
                  batchIndex += 1;
                }

                return sequentialBatchProcessingFuture.thenComposeAsync(
                    updatedCheckpoint -> {
                      if (updatedCheckpoint == null || StringUtils.isBlank(nextContinuationToken)) {
                        // there was an error when uploading batch or we reached last page and all
                        // files have been uploaded
                        return CompletableFuture.completedFuture(updatedCheckpoint);
                      }
                      return uploadInstantsInTimelineInBatches(
                          tableId,
                          table,
                          bucketName,
                          prefix,
                          updatedCheckpoint,
                          commitTimelineType,
                          nextContinuationToken);
                    },
                    executorService);
              }
              // Case 2: Reached last page and all files have been uploaded
              else if (nextContinuationToken == null) {
                // we return the original checkpoint, as no files were uploaded
                return CompletableFuture.completedFuture(checkpoint);
              }
              // Case 3: currently retrieved page has already been processed in a previous run,
              // processing next page
              else {
                return uploadInstantsInTimelineInBatches(
                    tableId,
                    table,
                    bucketName,
                    prefix,
                    checkpoint,
                    commitTimelineType,
                    nextContinuationToken);
              }
            },
            executorService)
        .exceptionally(throwable -> null);
  }

  private CompletableFuture<Void> uploadBatch(
      UUID tableId, List<File> batch, CommitTimelineType commitTimelineType, String directoryUri) {
    List<String> commitInstants =
        batch.stream()
            .map(file -> getFileNameWithPrefix(file, commitTimelineType))
            .collect(Collectors.toList());
    return onehouseApiClient
        .generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(tableId.toString())
                .commitInstants(commitInstants)
                .commitTimelineType(commitTimelineType)
                .build())
        .thenComposeAsync(
            generateCommitMetadataUploadUrlResponse -> {
              if (generateCommitMetadataUploadUrlResponse.isFailure()) {
                throw new RuntimeException(
                    String.format(
                        "failed to generate presigned urls: status_code: %d exception: %s",
                        generateCommitMetadataUploadUrlResponse.getStatusCode(),
                        generateCommitMetadataUploadUrlResponse.getCause()));
              }

              List<CompletableFuture<Void>> uploadFutures = new ArrayList<>();
              for (int i = 0; i < batch.size(); i++) {
                uploadFutures.add(
                    presignedUrlFileUploader.uploadFileToPresignedUrl(
                        generateCommitMetadataUploadUrlResponse.getUploadUrls().get(i),
                        constructStorageUri(directoryUri, batch.get(i).getFilename())));
              }

              return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]));
            },
            executorService);
  }

  private CompletableFuture<Checkpoint> updateCheckpointAfterProcessingBatch(
      UUID tableId,
      Checkpoint previousCheckpoint,
      boolean processedAllBatchesInCurrentPage,
      File lastUploadedFile,
      List<String> filesUploaded,
      CommitTimelineType commitTimelineType,
      String currentContinuationToken,
      String nextContinuationToken) {

    // archived instants would already be processed if we are currently processing active timeline
    boolean archivedCommitsProcessed = true;
    int batchId = previousCheckpoint.getBatchId() + 1;
    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)) {
      // we have processed the last batch and no more pages available
      archivedCommitsProcessed = processedAllBatchesInCurrentPage && nextContinuationToken == null;
    }

    // if we have more batches left in current page, we use currentContinuationToken
    String continuationTokenToUseForNextBatch =
        processedAllBatchesInCurrentPage ? nextContinuationToken : currentContinuationToken;
    Checkpoint updatedCheckpoint =
        Checkpoint.builder()
            .batchId(batchId)
            .lastUploadedFile(lastUploadedFile.getFilename())
            .checkpointTimestamp(lastUploadedFile.getLastModifiedAt())
            .archivedCommitsProcessed(archivedCommitsProcessed)
            .continuationToken(continuationTokenToUseForNextBatch)
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
                      String.format(
                          "failed to update checkpoint: status_code: %d, exception: %s",
                          upsertTableMetricsCheckpointResponse.getStatusCode(),
                          upsertTableMetricsCheckpointResponse.getCause()));
                }
                return updatedCheckpoint;
              });
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new RuntimeException("failed to serialise checkpoint", e));
    }
  }

  /*
   *  filters out all instants that have been uploaded previously based on the checkpoint information
   */
  private List<File> getFilesToUploadBasedOnPreviousCheckpoint(
      List<File> filesList, Checkpoint checkpoint) {
    if (filesList.isEmpty()) {
      return filesList;
    }
    List<File> filteredAndSortedFiles =
        filesList.stream()
            .filter(file -> !file.isDirectory()) // filter out directories
            .filter(file -> !file.getLastModifiedAt().isBefore(checkpoint.getCheckpointTimestamp()))
            .filter(
                file ->
                    !file.getFilename()
                        .equals(HOODIE_PROPERTIES_FILE)) // will only be processed in batch 1
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

    List<File> filesToUpload =
        lastUploadedIndexOpt.isPresent()
            ? filteredAndSortedFiles.subList(
                lastUploadedIndexOpt.getAsInt() + 1, filteredAndSortedFiles.size())
            : filteredAndSortedFiles;

    if (checkpoint.getBatchId() == 0) {
      // for the first batch, always include hoodie properties file
      filesToUpload.add(0, HOODIE_PROPERTIES_FILE_OBJ);
    }

    return filesToUpload;
  }

  private String constructStorageUri(String directoryUri, String fileName) {
    if (HOODIE_PROPERTIES_FILE.equals(fileName)) {
      String archivedSuffix = ARCHIVED_FOLDER_NAME + '/';
      String hoodieDirectoryUri =
          directoryUri.endsWith(archivedSuffix)
              ? directoryUri.substring(0, directoryUri.length() - "archived/".length())
              : directoryUri;
      return storageUtils.constructFileUri(hoodieDirectoryUri, HOODIE_PROPERTIES_FILE);
    }
    return storageUtils.constructFileUri(directoryUri, fileName);
  }

  private String getPathSuffixForTimeline(CommitTimelineType commitTimelineType) {
    String pathSuffix = HOODIE_FOLDER_NAME + '/';
    return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        ? pathSuffix + ARCHIVED_FOLDER_NAME + '/'
        : pathSuffix;
  }

  private String getFileNameWithPrefix(File file, CommitTimelineType commitTimelineType) {
    String archivedPrefix = "archived/";
    return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
            && !HOODIE_PROPERTIES_FILE.equals(file.getFilename())
        ? archivedPrefix + file.getFilename()
        : file.getFilename();
  }

  @VisibleForTesting
  int getUploadBatchSize() {
    return PRESIGNED_URL_REQUEST_BATCH_SIZE;
  }
}
