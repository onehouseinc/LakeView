package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.ACTIVE_COMMIT_INSTANT_PATTERN;
import static ai.onehouse.constants.MetadataExtractorConstants.ARCHIVED_COMMIT_INSTANT_PATTERN;
import static ai.onehouse.constants.MetadataExtractorConstants.ARCHIVED_COMMIT_INSTANT_PATTERN_V2;
import static ai.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.HISTORY_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE_OBJ;
import static ai.onehouse.constants.MetadataExtractorConstants.MANIFEST_FILE_PREFIX;
import static ai.onehouse.constants.MetadataExtractorConstants.ROLLBACK_ACTION;
import static ai.onehouse.constants.MetadataExtractorConstants.SAVEPOINT_ACTION;
import static ai.onehouse.constants.MetadataExtractorConstants.TIMELINE_FOLDER_NAME;
import static ai.onehouse.constants.MetadataExtractorConstants.TIMELINE_LAYOUT_VERSION_V2;
import static ai.onehouse.constants.MetadataExtractorConstants.V1_ARCHIVED_NUMERIC_PATTERN;
import static ai.onehouse.constants.MetadataExtractorConstants.VERSION_MARKER_FILE;
import static ai.onehouse.metadata_extractor.ActiveTimelineInstantBatcher.areRelatedInstants;
import static ai.onehouse.metadata_extractor.ActiveTimelineInstantBatcher.areRelatedSavepointOrRollbackInstants;
import static ai.onehouse.metadata_extractor.ActiveTimelineInstantBatcher.getActiveTimeLineInstant;
import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;

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
import ai.onehouse.RuntimeModule.TableMetadataUploadObjectStorageAsyncClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

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
  private final LSMTimelineManifestReader lsmTimelineManifestReader;

  @Inject
  public TimelineCommitInstantsUploader(
      @Nonnull @TableMetadataUploadObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      @Nonnull PresignedUrlFileUploader presignedUrlFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService,
      @Nonnull ActiveTimelineInstantBatcher activeTimelineInstantBatcher,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics,
      @Nonnull Config config,
      @Nonnull LSMTimelineManifestReader lsmTimelineManifestReader) {
    this.asyncStorageClient = asyncStorageClient;
    this.presignedUrlFileUploader = presignedUrlFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.activeTimelineInstantBatcher = activeTimelineInstantBatcher;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
    this.extractorConfig = config.getMetadataExtractorConfig();
    this.lsmTimelineManifestReader = lsmTimelineManifestReader;
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
   * upload is finished.
   */
  public CompletableFuture<Checkpoint> batchUploadWithCheckpoint(
      String tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    log.info("uploading instants in table: {} timeline: {}", table, commitTimelineType);

    String timelineUri =
        storageUtils.constructFileUri(
            table.getAbsoluteTableUri(),
            getPathSuffixForTimeline(commitTimelineType, table.getTimelineLayoutVersion()));

    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        && table.getTimelineLayoutVersion() == TIMELINE_LAYOUT_VERSION_V2) {
      // V2 (Hudi 1.x) archived timeline lives under .hoodie/timeline/history/ as an LSM tree.
      // We mirror it manifest-by-manifest rather than listing files: the manifest is the only
      // source of truth for which parquet files are live, and filename-based checkpointing is
      // unsafe because compaction rewrites the file set between runs.
      return executeManifestDrivenArchivedUpload(tableId, table, timelineUri, checkpoint);
    }

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
   * paginated upload.
   */
  public CompletableFuture<Checkpoint> paginatedBatchUploadWithCheckpoint(
      String tableId, Table table, Checkpoint checkpoint, CommitTimelineType commitTimelineType) {
    log.info("uploading instants in table: {} timeline: {}", table, commitTimelineType);
    String bucketName = storageUtils.getBucketNameFromUri(table.getAbsoluteTableUri());
    String prefix =
        storageUtils.getPathFromUrl(
            storageUtils.constructFileUri(
                table.getAbsoluteTableUri(),
                getPathSuffixForTimeline(
                    commitTimelineType, table.getTimelineLayoutVersion())));

    // startAfter is used only in the first call to get the objects, post that continuation token is
    // used
    // Resetting the firstIncompleteCommitFile so that we do not process from the same commit again
    // All commit files will be processed after firstIncompleteCommitFile, and the checkpoint will be
    // updated accordingly
    String startAfter = getStartAfterString(prefix, checkpoint, true);
    return executePaginatedBatchUpload(
        tableId,
        table,
        bucketName,
        prefix,
        checkpoint.toBuilder().firstIncompleteCommitFile("").build(),
        commitTimelineType,
        startAfter);
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
                  getMetadataExtractorFailureReason(
                      throwable,
                      MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                  String.format("Exception when uploading instants for table %s timeline %s: %s", table, commitTimelineType, throwable.getMessage()));
              return null; // handled in uploadNewInstantsSinceCheckpoint function
            });
  }

  /**
   * V2 archived upload, manifest-driven.
   *
   * <p>Reads {@code _version_} and the latest {@code manifest_N} from {@code
   * .hoodie/timeline/history/}. Uploads only parquet files newly referenced by the current
   * manifest (diffed against the previous manifest when available, otherwise bootstrapped from
   * scratch), then uploads the new manifest, then the {@code _version_} marker last so the
   * backend never sees a manifest pointing at files that have not arrived yet. Checkpoints by
   * manifest version, not filename, which makes the path compaction-safe: compaction merges L0
   * files into a single higher-level file, but the new manifest references the merged file and
   * the backend (which reads via {@code TimelineFactory.createArchivedTimeline}, see
   * gateway-controller PR 8797) honours the manifest, so any orphaned mirror copies are inert.
   */
  private CompletableFuture<Checkpoint> executeManifestDrivenArchivedUpload(
      String tableId, Table table, String historyUri, Checkpoint checkpoint) {
    return lsmTimelineManifestReader
        .readLatestManifest(historyUri)
        .thenComposeAsync(
            currentSnapshot ->
                processManifestSnapshot(
                    tableId, table, historyUri, checkpoint, currentSnapshot),
            executorService)
        .exceptionally(
            throwable -> {
              log.error(
                  "Encountered exception when uploading V2 archived timeline for table {}",
                  table,
                  throwable);
              hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
                  getMetadataExtractorFailureReason(
                      throwable,
                      MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                  String.format(
                      "Exception when uploading V2 archived timeline for table %s: %s",
                      table, throwable.getMessage()));
              return null;
            });
  }

  private CompletableFuture<Checkpoint> processManifestSnapshot(
      String tableId,
      Table table,
      String historyUri,
      Checkpoint checkpoint,
      LSMTimelineManifestReader.ManifestSnapshot currentSnapshot) {
    if (currentSnapshot.isEmpty()) {
      log.info(
          "No V2 archived timeline yet for table {} (no _version_ file). Skipping.", table);
      return CompletableFuture.completedFuture(
          checkpoint.toBuilder().archivedCommitsProcessed(true).build());
    }

    int previousVersion = checkpoint.getLastArchivedManifestVersion();
    int currentVersion = currentSnapshot.getVersion();
    if (currentVersion == previousVersion && checkpoint.isArchivedCommitsProcessed()) {
      log.info(
          "V2 archived timeline already at manifest_{} for table {}; nothing to do.",
          currentVersion,
          table);
      return CompletableFuture.completedFuture(checkpoint);
    }

    CompletableFuture<Set<String>> previouslyMirroredFuture =
        previousVersion > 0
            ? lsmTimelineManifestReader
                .readManifestFileNames(historyUri, previousVersion)
                .thenApply(HashSet::new)
            : CompletableFuture.completedFuture(new HashSet<>());

    return previouslyMirroredFuture.thenComposeAsync(
        previouslyMirrored -> {
          List<File> filesToUpload =
              buildV2ArchivedUploadList(checkpoint, currentSnapshot, previouslyMirrored,
                  currentVersion);
          int newParquetCount = filesToUpload.size() - 2
              - (checkpoint.getBatchId() == 0 ? 1 : 0);

          log.info(
              "V2 archived: mirroring {} new parquet(s) plus manifest_{} for table {}"
                  + " (previous mirrored manifest version: {})",
              newParquetCount,
              currentVersion,
              table,
              previousVersion);

          return uploadV2ArchivedFilesInBatches(
              tableId, table, filesToUpload, checkpoint, currentVersion);
        },
        executorService);
  }

  private List<File> buildV2ArchivedUploadList(
      Checkpoint checkpoint,
      LSMTimelineManifestReader.ManifestSnapshot currentSnapshot,
      Set<String> previouslyMirrored,
      int currentVersion) {
    List<File> filesToUpload = new ArrayList<>();
    if (checkpoint.getBatchId() == 0) {
      filesToUpload.add(HOODIE_PROPERTIES_FILE_OBJ);
    }
    for (String parquet : currentSnapshot.getParquetFileNames()) {
      if (!previouslyMirrored.contains(parquet)) {
        filesToUpload.add(buildArchivedFile(parquet));
      }
    }
    // Upload order: parquets -> manifest -> _version_. The manifest must arrive after every
    // parquet it references, and _version_ must be the very last write so a partial run never
    // advertises an inconsistent snapshot to the backend.
    filesToUpload.add(buildArchivedFile(MANIFEST_FILE_PREFIX + currentVersion));
    filesToUpload.add(buildArchivedFile(VERSION_MARKER_FILE));
    return filesToUpload;
  }

  private static File buildArchivedFile(String filename) {
    return File.builder()
        .filename(filename)
        .isDirectory(false)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }

  /**
   * Uploads the V2 archived file list in sequential batches and writes a single checkpoint after
   * the final batch succeeds. The checkpoint advances {@code lastArchivedManifestVersion} so the
   * next run knows which manifest to diff against.
   */
  private CompletableFuture<Checkpoint> uploadV2ArchivedFilesInBatches(
      String tableId,
      Table table,
      List<File> filesToUpload,
      Checkpoint previousCheckpoint,
      int newManifestVersion) {
    int batchSize = getUploadBatchSize(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    List<List<File>> batches = Lists.partition(filesToUpload, batchSize);
    String directoryUri =
        storageUtils.constructFileUri(
            table.getAbsoluteTableUri(),
            getPathSuffixForTimeline(
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED, table.getTimelineLayoutVersion()));

    CompletableFuture<List<UploadedFile>> sequentialUpload =
        CompletableFuture.completedFuture(new ArrayList<>());
    for (List<File> batch : batches) {
      sequentialUpload =
          sequentialUpload.thenComposeAsync(
              accumulated -> {
                if (accumulated == null) {
                  return CompletableFuture.completedFuture(null);
                }
                return uploadBatch(
                        tableId,
                        batch,
                        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED,
                        directoryUri,
                        table.getTimelineLayoutVersion())
                    .thenApply(
                        ignored -> {
                          for (File f : batch) {
                            accumulated.add(
                                UploadedFile.builder()
                                    .name(
                                        getFileNameWithPrefix(
                                            f,
                                            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED,
                                            table.getTimelineLayoutVersion()))
                                    .lastModifiedAt(f.getLastModifiedAt().toEpochMilli())
                                    .build());
                          }
                          return accumulated;
                        })
                    .exceptionally(
                        throwable -> {
                          hudiMetadataExtractorMetrics
                              .incrementTableMetadataProcessingFailureCounter(
                                  getMetadataExtractorFailureReason(
                                      throwable,
                                      MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                                  String.format(
                                      "V2 archived batch upload failed for table %s: %s",
                                      table.getAbsoluteTableUri(), throwable.getMessage()));
                          log.error(
                              "V2 archived batch upload failed for table: {}. Skipping further"
                                  + " batches in this run.",
                              table.getAbsoluteTableUri(),
                              throwable);
                          return null;
                        });
              },
              executorService);
    }

    return sequentialUpload.thenComposeAsync(
        accumulated -> {
          if (accumulated == null || accumulated.isEmpty()) {
            return CompletableFuture.completedFuture(null);
          }
          return upsertV2ArchivedCheckpoint(
              tableId, filesToUpload, accumulated, previousCheckpoint, batches.size(),
              newManifestVersion);
        },
        executorService);
  }

  private CompletableFuture<Checkpoint> upsertV2ArchivedCheckpoint(
      String tableId,
      List<File> filesToUpload,
      List<UploadedFile> accumulated,
      Checkpoint previousCheckpoint,
      int batchCount,
      int newManifestVersion) {
    // The final file uploaded is _version_, which is also the marker we use as
    // lastUploadedFile for back-compat with the v1 checkpoint shape.
    File lastFile = filesToUpload.get(filesToUpload.size() - 1);
    Checkpoint updatedCheckpoint =
        previousCheckpoint
            .toBuilder()
            .batchId(previousCheckpoint.getBatchId() + batchCount)
            .lastUploadedFile(lastFile.getFilename())
            .checkpointTimestamp(lastFile.getLastModifiedAt())
            .archivedCommitsProcessed(true)
            .lastArchivedManifestVersion(newManifestVersion)
            .build();
    try {
      return onehouseApiClient
          .upsertTableMetricsCheckpoint(
              UpsertTableMetricsCheckpointRequest.builder()
                  .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                  .tableId(tableId)
                  .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                  .filesUploaded(
                      accumulated.stream()
                          .map(UploadedFile::getName)
                          .collect(Collectors.toList()))
                  .uploadedFiles(accumulated)
                  .build())
          .thenApply(
              response -> {
                if (response.isFailure()) {
                  throw new IllegalStateException(
                      String.format(
                          "failed to update checkpoint: status_code: %d, exception: %s",
                          response.getStatusCode(), response.getCause()));
                }
                hudiMetadataExtractorMetrics.incrementTablesProcessedCounter();
                return updatedCheckpoint;
              });
    } catch (JsonProcessingException e) {
      CompletableFuture<Checkpoint> failed = new CompletableFuture<>();
      failed.completeExceptionally(
          new IllegalStateException("failed to serialise checkpoint", e));
      return failed;
    }
  }

  private CompletableFuture<Checkpoint> executePaginatedBatchUpload(
      String tableId,
      Table table,
      String bucketName,
      String prefix,
      Checkpoint checkpoint,
      CommitTimelineType commitTimelineType,
      String startAfter) {
    return asyncStorageClient
        .fetchObjectsByPage(bucketName, prefix, null, startAfter)
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
                            hudiMetadataExtractorMetrics.incrementTablesProcessedCounter();
                            return CompletableFuture.completedFuture(checkpoint);
                          }
                          if (StringUtils.isBlank(nextContinuationToken)) {
                            log.info(
                                "Reached end of instants in {} for table {}",
                                commitTimelineType,
                                table);
                            hudiMetadataExtractorMetrics.incrementTablesProcessedCounter();
                            return CompletableFuture.completedFuture(updatedCheckpoint);
                          }
                          return executePaginatedBatchUpload(
                              tableId,
                              table,
                              bucketName,
                              prefix,
                              updatedCheckpoint,
                              commitTimelineType,
                              getStartAfterString(prefix, updatedCheckpoint, false));
                        },
                        executorService);
              } else {
                log.info("Reached end of instants in {} for table {}", commitTimelineType, table);
                hudiMetadataExtractorMetrics.incrementTablesProcessedCounter();
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
                  getMetadataExtractorFailureReason(
                      throwable,
                      MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                  String.format("Exception when uploading instants for table %s timeline %s: %s", table, commitTimelineType, throwable.getMessage()));
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
   * paginated upload. if upload fails for the batch then the function returns null instead
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
      Pair<String, List<List<File>>> incompleteCheckpointBatchesPair =
          activeTimelineInstantBatcher.createBatches(
              filesToUpload,
              getUploadBatchSize(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE),
              checkpoint);
      batches = incompleteCheckpointBatchesPair.getRight();
      checkpoint =
          checkpoint
              .toBuilder()
              .firstIncompleteCommitFile(incompleteCheckpointBatchesPair.getLeft())
              .build();
    }
    int numBatches = batches.size();

    if (numBatches == 0) {
      // In case of CONTINUE_ON_INCOMPLETE_COMMIT, the extractor also needs to check subsequent pages hence
      // returning a non-null checkpoint to continue processing.
      if (
          CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE.equals(commitTimelineType) &&
          extractorConfig
          .getUploadStrategy()
          .equals(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT)
      ) {
        log.info(
            "No batches found in current page for table {} timeline {}",
            table,
            commitTimelineType);
        return CompletableFuture.completedFuture(checkpoint);
      }
      log.info(
          "Could not create batches with completed commits for table {} timeline {}",
          table,
          commitTimelineType);
      return CompletableFuture.completedFuture(null);
    }

    int totalInstantsInBatches = batches.stream().mapToInt(List::size).sum();
    log.info(
        "Processing {} instants for tableId: {} table: {} timeline: {} lastUploadedFile: {} sequentially in {} batches totalling {} instants",
        filesToUpload.size(),
        tableId,
        table,
        commitTimelineType,
        checkpoint.getLastUploadedFile(),
        numBatches,
        totalInstantsInBatches);
    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE.equals(commitTimelineType)
        && totalInstantsInBatches < filesToUpload.size()) {
      log.warn(
          "{} instants read from customer bucket but excluded from upload for table {} timeline {} due to incomplete commits",
          filesToUpload.size() - totalInstantsInBatches,
          table,
          commitTimelineType);
      hudiMetadataExtractorMetrics.incrementIncompleteCommitInstantsSkippedCounter();
    }

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
                    "Uploading batch {} for tableId: {} table: {} timeline: {} commitCount: {}",
                    updatedCheckpoint.getBatchId() + 1,
                    tableId,
                    table,
                    commitTimelineType,
                    batch.size());
                return uploadBatch(
                    tableId,
                    batch,
                    commitTimelineType,
                    storageUtils.constructFileUri(
                        table.getAbsoluteTableUri(),
                        getPathSuffixForTimeline(
                            commitTimelineType, table.getTimelineLayoutVersion())),
                    table.getTimelineLayoutVersion())
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
                                                    getFileNameWithPrefix(
                                                        file, commitTimelineType,
                                                        table.getTimelineLayoutVersion()))
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
                                  getMetadataExtractorFailureReason(
                                      throwable,
                                      MetricsConstants.MetadataUploadFailureReasons.UNKNOWN),
                                  String.format("Error processing batch for table %s: %s", table.getAbsoluteTableUri(), throwable.getMessage()));
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
      String directoryUri,
      int timelineLayoutVersion) {
    List<String> commitInstants =
        batch.stream()
            .map(file -> getFileNameWithPrefix(file, commitTimelineType, timelineLayoutVersion))
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
                            constructStorageUri(
                                directoryUri, batch.get(i).getFilename(), timelineLayoutVersion),
                            extractorConfig.getFileUploadStreamBatchSize())
                        .thenApply(result -> {
                          hudiMetadataExtractorMetrics.incrementMetadataUploadSuccessCounter();
                          return result;
                        }));
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
            .firstIncompleteCommitFile(previousCheckpoint.getFirstIncompleteCommitFile())
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

  /**
   * Determines if a file should be included based on filters.
   */
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
        if (extractorConfig
            .getUploadStrategy()
            .equals(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT)) {
          // The commits can be incomplete even if below condition is true, hence not ignoring for
          // non-blocking mode
          return false;
        }
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
        || ARCHIVED_COMMIT_INSTANT_PATTERN.matcher(fileName).matches()
        || ARCHIVED_COMMIT_INSTANT_PATTERN_V2.matcher(fileName).matches();
  }

  private String constructStorageUri(
      String directoryUri, String fileName, int timelineLayoutVersion) {
    if (HOODIE_PROPERTIES_FILE.equals(fileName)) {
      String hoodieDirectoryUri = directoryUri;
      if (timelineLayoutVersion == TIMELINE_LAYOUT_VERSION_V2) {
        // V2: strip "timeline/history/" or "timeline/" to get back to .hoodie/
        String historySuffix = TIMELINE_FOLDER_NAME + '/' + HISTORY_FOLDER_NAME + '/';
        String timelineSuffix = TIMELINE_FOLDER_NAME + '/';
        if (directoryUri.endsWith(historySuffix)) {
          hoodieDirectoryUri =
              directoryUri.substring(0, directoryUri.length() - historySuffix.length());
        } else if (directoryUri.endsWith(timelineSuffix)) {
          hoodieDirectoryUri =
              directoryUri.substring(0, directoryUri.length() - timelineSuffix.length());
        }
      } else {
        // V1: strip "archived/" to get back to .hoodie/
        String archivedSuffix = ARCHIVED_FOLDER_NAME + '/';
        if (directoryUri.endsWith(archivedSuffix)) {
          hoodieDirectoryUri =
              directoryUri.substring(0, directoryUri.length() - archivedSuffix.length());
        }
      }
      return storageUtils.constructFileUri(hoodieDirectoryUri, HOODIE_PROPERTIES_FILE);
    }
    return storageUtils.constructFileUri(directoryUri, fileName);
  }

  private String getPathSuffixForTimeline(
      CommitTimelineType commitTimelineType, int timelineLayoutVersion) {
    String pathSuffix = HOODIE_FOLDER_NAME + '/';
    if (timelineLayoutVersion == TIMELINE_LAYOUT_VERSION_V2) {
      pathSuffix += TIMELINE_FOLDER_NAME + '/';
      return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
          ? pathSuffix + HISTORY_FOLDER_NAME + '/'
          : pathSuffix;
    }
    return CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        ? pathSuffix + ARCHIVED_FOLDER_NAME + '/'
        : pathSuffix;
  }

  private String getFileNameWithPrefix(
      File file, CommitTimelineType commitTimelineType, int timelineLayoutVersion) {
    if (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
        && !HOODIE_PROPERTIES_FILE.equals(file.getFilename())) {
      if (timelineLayoutVersion == TIMELINE_LAYOUT_VERSION_V2) {
        return TIMELINE_FOLDER_NAME + '/' + HISTORY_FOLDER_NAME + '/' + file.getFilename();
      }
      return ARCHIVED_FOLDER_NAME + '/' + file.getFilename();
    }
    if (timelineLayoutVersion == TIMELINE_LAYOUT_VERSION_V2
        && !HOODIE_PROPERTIES_FILE.equals(file.getFilename())) {
      return TIMELINE_FOLDER_NAME + '/' + file.getFilename();
    }
    return file.getFilename();
  }

  private BigDecimal getCommitIdFromActiveTimelineInstant(String activeTimeLineInstant) {
    String timestampPart = activeTimeLineInstant.split("\\.")[0];
    if (timestampPart.contains("_")) {
      timestampPart = timestampPart.split("_")[0];
    }
    return new BigDecimal(timestampPart);
  }

  private long getNumericPartFromArchivedCommit(String archivedCommitFileName) {
    // V1 archive file: .commits_.archive.5_20260101-20260115-50
    Matcher v1Matcher = V1_ARCHIVED_NUMERIC_PATTERN.matcher(archivedCommitFileName);
    if (v1Matcher.find()) {
      return Long.parseLong(v1Matcher.group(1));
    }
    throw new IllegalArgumentException(
        "invalid archived commit file type: " + archivedCommitFileName);
  }

  public String getStartAfterString(String prefix, Checkpoint checkpoint, boolean isFirstFetch) {
    String lastProcessedFile = checkpoint.getLastUploadedFile();
    // Base case to process from the beginning
    if (lastProcessedFile.equals(HOODIE_PROPERTIES_FILE)
        || StringUtils.isBlank(lastProcessedFile)) {
      return null;
    }

    // Extractor blocks on incomplete commits, startAfter is the last processed file
    if (extractorConfig
        .getUploadStrategy()
        .equals(MetadataExtractorConfig.UploadStrategy.BLOCK_ON_INCOMPLETE_COMMIT)
        || !isFirstFetch) {
      return storageUtils.constructFileUri(prefix, lastProcessedFile);
    }

    // Extractor does not block on incomplete commits, it resumes from the first incomplete commit
    // file if present else takes the lastProcessedFile as the starting point
    String firstIncompleteCommitFile = checkpoint.getFirstIncompleteCommitFile();
    return StringUtils.isBlank(firstIncompleteCommitFile)
        ? storageUtils.constructFileUri(prefix, lastProcessedFile)
        : storageUtils.constructFileUri(prefix, firstIncompleteCommitFile);
  }

  /**
   * Extracts the last uploaded file from batch. If the commit timeline type is ARCHIVED, we return
   * the last file in the batch. If the batch only contains hoodie.properties file, we return
   * hoodie.properties. If the batch ends with savepoint commit, we return the second to last item.
   * If the batch ends with other commit types, we return third to last item.
   */
  public File getLastUploadedFileFromBatch(
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

    if (isRollbackCommit(batch.get(batch.size() - 1))) {
      int lastIndex = batch.size() - 1;
      ActiveTimelineInstantBatcher.ActiveTimelineInstant lastInstant =
          getActiveTimeLineInstant(batch.get(lastIndex).getFilename());
      
      // Case 1: Full rollback sequence (xyz.rollback, xyz.rollback.inflight, xyz.rollback.requested)
      if (lastIndex >= 2 &&
          areRelatedInstants(lastInstant,
              getActiveTimeLineInstant(batch.get(lastIndex-1).getFilename()),
              getActiveTimeLineInstant(batch.get(lastIndex-2).getFilename()))) {
        return batch.get(lastIndex - 2);
      }
      
      // Case 2: Rollback with inflight (xyz.rollback, xyz.inflight)
      if (lastIndex >= 1 &&
          areRelatedSavepointOrRollbackInstants(lastInstant,
              getActiveTimeLineInstant(batch.get(lastIndex-1).getFilename()))) {
        return batch.get(lastIndex - 1);
      }
      
      // Case 3: Simple rollback (xyz.rollback)
      return batch.get(lastIndex);
    }

    return batch.get(batch.size() - 3);
  }

  private boolean isSavepointCommit(File file) {
    return hasActionType(file, SAVEPOINT_ACTION);
  }

  private boolean isRollbackCommit(File file) {
    return hasActionType(file, ROLLBACK_ACTION);
  }

  private boolean hasActionType(File file, String actionType) {
    String[] parts = file.getFilename().split("\\.", 3);
    return parts.length >= 2 && actionType.equals(parts[1]);
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
