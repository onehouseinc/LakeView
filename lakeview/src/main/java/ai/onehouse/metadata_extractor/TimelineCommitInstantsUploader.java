package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.ACTIVE_COMMIT_INSTANT_PATTERN;
import static ai.onehouse.constants.MetadataExtractorConstants.ARCHIVED_COMMIT_INSTANT_PATTERN;
import static ai.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE_OBJ;
import static ai.onehouse.constants.MetadataExtractorConstants.SAVEPOINT_ACTION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import ai.onehouse.api.OnehouseApiClient;
import ai.onehouse.api.models.request.CommitTimelineType;
import ai.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import ai.onehouse.api.models.request.UploadedFile;
import ai.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.PresignedUrlFileUploader;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  private final MetadataExtractorConfig extractorConfig;

  @Inject
  public TimelineCommitInstantsUploader(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService,
      @Nonnull ActiveTimelineInstantBatcher activeTimelineInstantBatcher,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics,
      @Nonnull Config config) {
    this.asyncStorageClient = asyncStorageClient;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.activeTimelineInstantBatcher = activeTimelineInstantBatcher;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
    this.extractorConfig = config.getMetadataExtractorConfig();
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
   * @return A future that completes with a new checkpoint after the
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
   * @return A future that completes with a new checkpoint after each
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
                      files, checkpoint, commitTimelineType, false);

              return filesToUpload.isEmpty()
                  ? CompletableFuture.completedFuture(checkpoint)
                  : uploadInstantsInSequentialBatches(
                      tableId, table, filesToUpload, checkpoint, commitTimelineType);
            },
            executorService)
        .exceptionally(
            throwable -> {
              log.error(
                  "Encountered exception when uploading instants for table {} timeline {}",
                  table,
                  commitTimelineType,
                  throwable);
              hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
                  MetricsConstants.MetadataUploadFailureReasons.UNKNOWN);
              return null; // handled in uploadNewInstantsSinceCheckpoint function
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
                        tableId, table, filesToUpload, checkpoint, commitTimelineType)
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
              hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
                  MetricsConstants.MetadataUploadFailureReasons.UNKNOWN);
              return null; // handled in uploadNewInstantsSinceCheckpoint
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
   * @return CompletableFuture<Checkpoint> A future that completes with a new checkpoint after each
   *     paginated upload. if upload fails for the batch then the function returns null instead
   */
  private CompletableFuture<Checkpoint> uploadInstantsInSequentialBatches(
      String tableId,
      Table table,
      List<File> filesToUpload,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType) {
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
      log.info(
          "Could not create batches with completed commits for table {} timeline {}",
          table,
          commitTimelineType);
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
    for (List<File> batch : batches) {
      sequentialBatchProcessingFuture =
          sequentialBatchProcessingFuture.thenComposeAsync(
              updatedCheckpoint -> {
                if (updatedCheckpoint == null) {
                  return CompletableFuture.completedFuture(null);
                }

                File lastUploadedFile = getLastUploadedFileFromBatch(commitTimelineType, batch);
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
                                lastUploadedFile,
                                batch.stream()
                                    .map(
                                        file ->
                                            UploadedFile.builder()
                                                .name(
                                                    getFileNameWithPrefix(file, commitTimelineType))
                                                .lastModifiedAt(
                                                    file.getLastModifiedAt().toEpochMilli())
                                                .build())
                                    .collect(Collectors.toList()),
                                commitTimelineType),
                        executorService)
                    .exceptionally(
                        throwable -> {
                          hudiMetadataExtractorMetrics
                              .incrementTableMetadataProcessingFailureCounter(
                                  MetricsConstants.MetadataUploadFailureReasons.UNKNOWN);
                          log.error(
                              "error processing batch for table: {}. Skipping processing of further batches of table in current run.",
                              table.getAbsoluteTableUri(),
                              throwable);
                          return null;
                        });
              },
              executorService);
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
                .tableId(tableId)
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
                        constructStorageUri(directoryUri, batch.get(i).getFilename()),
                        extractorConfig.getFileUploadStreamBatchSize()));
              }

              return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]));
            },
            executorService);
  }

  private CompletableFuture<Checkpoint> updateCheckpointAfterProcessingBatch(
      String tableId,
      Checkpoint previousCheckpoint,
      File lastUploadedFile,
      List<UploadedFile> uploadedFiles,
      CommitTimelineType commitTimelineType) {

    // archived instants would be processed if we are currently processing the first batch of active
    // timeline
    boolean archivedCommitsProcessed = false;
    int batchId = previousCheckpoint.getBatchId() + 1;

    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE.equals(commitTimelineType)) {
      // we have processed atleast 1 batch of active timeline, (hence archived timeline must be
      // fully processed)
      archivedCommitsProcessed = true;
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
                  .tableId(tableId)
                  .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                  .filesUploaded(
                      uploadedFiles.stream()
                          .map(UploadedFile::getName)
                          .collect(Collectors.toList()))
                  .uploadedFiles(uploadedFiles)
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

    List<File> filesToUpload =
        filesList.stream()
            .filter(
                file ->
                    shouldIncludeFile(
                        file, checkpoint, applyLastModifiedAtFilter, commitTimelineType))
            .sorted(fileComparator)
            .collect(Collectors.toList());

    if (checkpoint.getBatchId() == 0) {
      // for the first batch, always include hoodie properties file
      filesToUpload.add(0, HOODIE_PROPERTIES_FILE_OBJ);
    }

    return filesToUpload;
  }

  /** Determines if a file should be included based on filters. */
  private boolean shouldIncludeFile(
      File file,
      Checkpoint checkpoint,
      boolean applyLastModifiedAtFilter,
      CommitTimelineType commitTimelineType) {
    return !file.isDirectory()
        && (!file.getLastModifiedAt().isBefore(checkpoint.getCheckpointTimestamp())
            || !applyLastModifiedAtFilter)
        && isInstantFile(file.getFilename())
        && !isInstantAlreadyUploaded(checkpoint, file, commitTimelineType)
        && !file.getFilename().equals(HOODIE_PROPERTIES_FILE)
        && StringUtils.isNotBlank(file.getFilename());
  }

  private boolean isInstantAlreadyUploaded(
      Checkpoint checkpoint, File file, CommitTimelineType commitTimelineType) {
    if (checkpoint.getBatchId() != 0 && isInstantFile(checkpoint.getLastUploadedFile())) {
      if (commitTimelineType.equals(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)) {
        return getCommitIdFromActiveTimelineInstant(file.getFilename())
                .compareTo(getCommitIdFromActiveTimelineInstant(checkpoint.getLastUploadedFile()))
            <= 0;
      } else {
        return getNumericPartFromArchivedCommit(file.getFilename())
            <= getNumericPartFromArchivedCommit(checkpoint.getLastUploadedFile());
      }
    }
    return false;
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

  private BigDecimal getCommitIdFromActiveTimelineInstant(String activeTimeLineInstant) {
    return new BigDecimal(activeTimeLineInstant.split("\\.")[0]);
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

  /**
   * Extracts the last uploaded file from batch. If the commit timeline type is ARCHIVED, we return
   * the last file in the batch. If the batch only contains hoodie.properties file, we return
   * hoodie.properties. If the batch ends with savepoint commit, we return the second to last item.
   * If the batch ends with other commit types, we return third to last item.
   */
  private File getLastUploadedFileFromBatch(
      CommitTimelineType commitTimelineType, List<File> batch) {
    if (commitTimelineType == CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED) {
      return batch.get(batch.size() - 1);
    }
    if (batch.size() == 1 && batch.get(0).getFilename().equals(HOODIE_PROPERTIES_FILE)) {
      return batch.get(0);
    }

    if (isSavepointCommit(batch.get(batch.size() - 1))) {
      return batch.get(batch.size() - 2);
    }

    return batch.get(batch.size() - 3);
  }

  private boolean isSavepointCommit(File file) {
    String[] parts = file.getFilename().split("\\.", 3);
    if (parts.length < 2) {
      return false;
    }
    return SAVEPOINT_ACTION.equals(parts[1]);
  }

  @VisibleForTesting
  int getUploadBatchSize(CommitTimelineType commitTimelineType) {
    if (commitTimelineType == CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED) {
      return extractorConfig.getPresignedUrlRequestBatchSizeArchivedTimeline();
    } else {
      return extractorConfig.getPresignedUrlRequestBatchSizeActiveTimeline();
    }
  }
}
