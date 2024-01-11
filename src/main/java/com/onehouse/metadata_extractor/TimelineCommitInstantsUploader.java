package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ACTIVE_COMMIT_INSTANT_PATTERN;
import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_COMMIT_INSTANT_PATTERN;
import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE_OBJ;
import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE;
import static com.onehouse.constants.MetadataExtractorConstants.PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private final ActiveTimelineInstantBatcher activeTimelineInstantBatcher;

  @Inject
  TimelineCommitInstantsUploader(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService,
      @Nonnull ActiveTimelineInstantBatcher activeTimelineInstantBatcher) {
    this.asyncStorageClient = asyncStorageClient;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.activeTimelineInstantBatcher = activeTimelineInstantBatcher;
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
  }

  /**
   * Performs a batch upload of instants from a specified timeline. Initially, it lists all instants
   * in the timeline and filters out those that have already been processed, based on the provided
   * checkpoint. It then uploads the remaining, new instants. This function is useful in scenarios
   * where instants in the timeline are not ordered by their filenames, such as in archived
   * timelines.
   *
   * @param tableId Unique identifier of the table.
   * @param table The table object.
   * @param checkpoint Checkpoint object used to track already processed instants.
   * @param commitTimelineType Type of the commit timeline.
   * @return CompletableFuture<Checkpoint> A future that completes with a new checkpoint after the
   *     upload is finished.
   */
  public CompletableFuture<Checkpoint> batchUploadWithCheckpoint(
      String tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    log.info("uploading instants in table: {} timeline: {}", table, commitTimelineType);

    String timelineUri =
        storageUtils.constructFileUri(
            table.getAbsoluteTableUri(), getPathSuffixForTimeline(commitTimelineType));

    return executeFullBatchUpload(tableId, table, timelineUri, checkpoint, commitTimelineType);
  }

  /**
   * Uploads instants in a timeline in a paginated manner. It lists a page of instants from the
   * timeline, uploads those that have not been previously processed based on the provided
   * checkpoint in batches, and then continues to the next page. This process repeats until the last
   * page is reached. This approach is recommended when instants are ordered by their filenames,
   * which is typical in active timelines.
   *
   * @param tableId Unique identifier of the table.
   * @param table The table object.
   * @param checkpoint Checkpoint object used to track already processed instants.
   * @param commitTimelineType Type of the commit timeline.
   * @return CompletableFuture<Checkpoint> A future that completes with a new checkpoint after each
   *     paginated upload.
   */
  public CompletableFuture<Checkpoint> paginatedBatchUploadWithCheckpoint(
      String tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    log.info("uploading instants in table: {} timeline: {}", table, commitTimelineType);
    String bucketName = storageUtils.getBucketNameFromUri(table.getAbsoluteTableUri());
    String prefix =
        storageUtils.getPathFromUrl(
            storageUtils.constructFileUri(
                table.getAbsoluteTableUri(), getPathSuffixForTimeline(commitTimelineType)));

    return executePaginatedBatchUpload(
        tableId, table, bucketName, prefix, checkpoint, commitTimelineType);
  }

  private CompletableFuture<Checkpoint> executeFullBatchUpload(
      String tableId,
      Table table,
      String timelineUri,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType) {
    return asyncStorageClient
        .listAllFilesInDir(timelineUri)
        .thenComposeAsync(
            files -> {
              List<File> filesToUpload =
                  getFilesToUploadBasedOnPreviousCheckpoint(
                      files, checkpoint, commitTimelineType, true);

              return filesToUpload.isEmpty()
                  ? CompletableFuture.completedFuture(checkpoint)
                  : uploadInstantsInSequentialBatches(
                      tableId, table, filesToUpload, checkpoint, commitTimelineType, true);
            },
            executorService)
        .exceptionally(
            throwable -> {
              log.error(
                  "Encountered exception when uploading instants for table {} timeline {}",
                  table,
                  commitTimelineType,
                  throwable);
              return null;
            });
  }

  private CompletableFuture<Checkpoint> executePaginatedBatchUpload(
      String tableId,
      Table table,
      String bucketName,
      String prefix,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType) {
    return asyncStorageClient
        .fetchObjectsByPage(bucketName, prefix, null, getStartAfterString(prefix, checkpoint))
        .thenComposeAsync(
            continuationTokenAndFiles -> {
              String nextContinuationToken = continuationTokenAndFiles.getLeft();
              List<File> filesToUpload =
                  getFilesToUploadBasedOnPreviousCheckpoint(
                      continuationTokenAndFiles.getRight(), checkpoint, commitTimelineType, false);

              if (!filesToUpload.isEmpty()) {
                return uploadInstantsInSequentialBatches(
                        tableId,
                        table,
                        filesToUpload,
                        checkpoint,
                        commitTimelineType,
                        nextContinuationToken == null)
                    .thenComposeAsync(
                        updatedCheckpoint -> {
                          if (updatedCheckpoint == null) {
                            // no batches to process, returning existing checkpoint
                            return CompletableFuture.completedFuture(checkpoint);
                          }
                          if (StringUtils.isBlank(nextContinuationToken)) {
                            log.info(
                                "Reached end of instants in {} for table {}",
                                commitTimelineType,
                                table);
                            return CompletableFuture.completedFuture(updatedCheckpoint);
                          }
                          return executePaginatedBatchUpload(
                              tableId,
                              table,
                              bucketName,
                              prefix,
                              updatedCheckpoint,
                              commitTimelineType);
                        },
                        executorService);
              } else {
                log.info("Reached end of instants in {} for table {}", commitTimelineType, table);
                return CompletableFuture.completedFuture(checkpoint);
              }
            },
            executorService)
        .exceptionally(
            throwable -> {
              log.error(
                  "Encountered exception when uploading instants for table {} timeline {}",
                  table,
                  commitTimelineType,
                  throwable);
              return null;
            });
  }

  /**
   * Executes a sequential upload of files in parallel batches. This function processes multiple
   * files in parallel within each batch, ensuring efficient use of resources. However, it maintains
   * a sequential order between batches, where a new batch of files is uploaded only after the
   * successful completion of the previous batch. This approach balances the benefits of parallel
   * processing with the need for sequential control, making it suitable for scenarios where order
   * of batch completion is important.
   *
   * @param tableId Unique identifier for the table associated with the files.
   * @param table The table object.
   * @param filesToUpload List of files to be uploaded.
   * @param checkpoint Checkpoint object used to track already processed instants.
   * @param commitTimelineType Type of the commit timeline.
   * @param isLastPage whether we are in the last page
   * @return CompletableFuture<Checkpoint> A future that completes with a new checkpoint after each
   *     paginated upload.
   */
  private CompletableFuture<Checkpoint> uploadInstantsInSequentialBatches(
      String tableId,
      Table table,
      List<File> filesToUpload,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType,
      boolean isLastPage) {
    List<List<File>> batches;
    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)) {
      batches =
          Lists.partition(
              filesToUpload, getUploadBatchSize(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED));
    } else {
      batches =
          activeTimelineInstantBatcher.createBatches(
              filesToUpload, getUploadBatchSize(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE));
    }
    int numBatches = batches.size();

    if (numBatches == 0) {
      return CompletableFuture.completedFuture(null);
    }

    log.info(
        "Processing {} instants in table {} timeline {} sequentially in {} batches",
        filesToUpload.size(),
        table,
        commitTimelineType,
        numBatches);

    CompletableFuture<Checkpoint> sequentialBatchProcessingFuture =
        CompletableFuture.completedFuture(checkpoint);
    int batchIndex = 0;
    for (List<File> batch : batches) {
      boolean processedAllBatchesInCurrentPage = (batchIndex >= numBatches - 1);
      sequentialBatchProcessingFuture =
          sequentialBatchProcessingFuture.thenComposeAsync(
              updatedCheckpoint -> {
                if (updatedCheckpoint == null) {
                  return CompletableFuture.completedFuture(null);
                }

                File lastUploadedFile =
                    CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
                        ? batch.get(batch.size() - 1)
                        : batch.get(
                            batch.size()
                                - 3); // third last item in the batch will be the completed instant
                log.info(
                    "uploading batch {} for table {} timeline: {}",
                    updatedCheckpoint.getBatchId() + 1,
                    table,
                    commitTimelineType);
                return uploadBatch(
                        tableId,
                        batch,
                        commitTimelineType,
                        storageUtils.constructFileUri(
                            table.getAbsoluteTableUri(),
                            getPathSuffixForTimeline(commitTimelineType)))
                    .thenComposeAsync(
                        ignored2 ->
                            updateCheckpointAfterProcessingBatch(
                                tableId,
                                updatedCheckpoint,
                                processedAllBatchesInCurrentPage,
                                lastUploadedFile,
                                batch.stream()
                                    .map(file -> getFileNameWithPrefix(file, commitTimelineType))
                                    .collect(Collectors.toList()),
                                commitTimelineType,
                                isLastPage),
                        executorService)
                    .exceptionally(
                        throwable -> {
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
    return sequentialBatchProcessingFuture;
  }

  private CompletableFuture<Void> uploadBatch(
      String tableId,
      List<File> batch,
      CommitTimelineType commitTimelineType,
      String directoryUri) {
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
      String tableId,
      Checkpoint previousCheckpoint,
      boolean processedAllBatchesInCurrentPage,
      File lastUploadedFile,
      List<String> filesUploaded,
      CommitTimelineType commitTimelineType,
      boolean isLastPage) {

    // archived instants would already be processed if we are currently processing active timeline
    boolean archivedCommitsProcessed = true;
    int batchId = previousCheckpoint.getBatchId() + 1;

    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)) {
      // we have processed the last batch and no more pages available
      archivedCommitsProcessed = processedAllBatchesInCurrentPage && isLastPage;
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
                      String.format(
                          "failed to update checkpoint: status_code: %d, exception: %s",
                          upsertTableMetricsCheckpointResponse.getStatusCode(),
                          upsertTableMetricsCheckpointResponse.getCause()));
                }
                return updatedCheckpoint;
              });
    } catch (JsonProcessingException e) {
      CompletableFuture<Checkpoint> f = new CompletableFuture<>();
      f.completeExceptionally(new RuntimeException("failed to serialise checkpoint", e));
      return f;
    }
  }

  /**
   * Filters out already uploaded files based on checkpoint information and sorts the remaining
   * files. This function filters and sorts files from a given list, considering their last modified
   * time and filename. It is used to determine which files need to be uploaded in the current run.
   *
   * @param filesList List of files to be filtered and sorted.
   * @param checkpoint Checkpoint object containing information about previously uploaded files.
   * @param commitTimelineType Type of the commit timeline (active or archived).
   * @param applyLastModifiedAtFilter Flag to apply last modified timestamp filter.
   * @return List<File> List of filtered and sorted files ready for upload.
   */
  private List<File> getFilesToUploadBasedOnPreviousCheckpoint(
      List<File> filesList,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType,
      boolean applyLastModifiedAtFilter) {
    if (filesList.isEmpty()) {
      return filesList;
    }
    Comparator<File> fileComparator;

    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE.equals(commitTimelineType)) {
      fileComparator = Comparator.comparing(File::getFilename);
    } else {
      fileComparator =
          Comparator.comparing(file -> getNumericPartFromArchivedCommit(file.getFilename()));
    }

    List<File> filteredAndSortedFiles =
        filesList.stream()
            .filter(file -> shouldIncludeFile(file, checkpoint, applyLastModifiedAtFilter))
            .sorted(fileComparator)
            .collect(Collectors.toList());

    // index of the last file which was uploaded
    OptionalInt lastUploadedIndexOpt =
        StringUtils.isNotBlank(checkpoint.getLastUploadedFile())
            ? IntStream.range(0, filteredAndSortedFiles.size())
                .filter(
                    i ->
                        filteredAndSortedFiles
                            .get(i)
                            .getFilename()
                            .startsWith(checkpoint.getLastUploadedFile()))
                .reduce((first, second) -> second) // finding last occurrence
            : OptionalInt.empty();

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

  /** Determines if a file should be included based on filters. */
  private boolean shouldIncludeFile(
      File file, Checkpoint checkpoint, boolean applyLastModifiedAtFilter) {
    return !file.isDirectory()
        && (!file.getLastModifiedAt().isBefore(checkpoint.getCheckpointTimestamp())
            || !applyLastModifiedAtFilter)
        && isInstantFile(file.getFilename())
        && !file.getFilename().equals(HOODIE_PROPERTIES_FILE)
        && StringUtils.isNotBlank(file.getFilename());
  }

  private boolean isInstantFile(String fileName) {
    return ACTIVE_COMMIT_INSTANT_PATTERN.matcher(fileName).matches()
        || ARCHIVED_COMMIT_INSTANT_PATTERN.matcher(fileName).matches();
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

  private int getNumericPartFromArchivedCommit(String archivedCommitFileName) {
    Pattern pattern = Pattern.compile("\\.archive\\.(\\d+)_");
    Matcher matcher = pattern.matcher(archivedCommitFileName);

    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    } else {
      throw new IllegalArgumentException("invalid archived commit file type");
    }
  }

  private String getStartAfterString(String prefix, Checkpoint checkpoint) {
    String lastProcessedFile = checkpoint.getLastUploadedFile();
    return lastProcessedFile.equals(HOODIE_PROPERTIES_FILE)
            || StringUtils.isBlank(lastProcessedFile)
        ? null
        : storageUtils.constructFileUri(prefix, checkpoint.getLastUploadedFile());
  }

  @VisibleForTesting
  int getUploadBatchSize(CommitTimelineType commitTimelineType) {
    if (commitTimelineType == CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED) {
      return PRESIGNED_URL_REQUEST_BATCH_SIZE_ARCHIVED_TIMELINE;
    } else {
      return PRESIGNED_URL_REQUEST_BATCH_SIZE_ACTIVE_TIMELINE;
    }
  }
}
