package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ACTIVE_TIMELINE_CHECKPOINT;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ARCHIVED_TIMELINE_CHECKPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.PresignedUrlFileUploader;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimelineCommitInstantsUploaderTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  @Mock private PresignedUrlFileUploader presignedUrlFileUploader;
  @Mock private OnehouseApiClient onehouseApiClient;
  private TimelineCommitInstantsUploader timelineCommitInstantsUploader;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final String S3_TABLE_URI = "s3://bucket/table/";
  private static final String ARCHIVED_FOLDER_PREFIX = "archived/";
  private static final Table TABLE =
      Table.builder()
          .absoluteTableUri(S3_TABLE_URI)
          .relativeTablePath("table")
          .databaseName("database")
          .lakeName("lake")
          .build();
  ;
  private static final UUID TABLE_ID = UUID.nameUUIDFromBytes(S3_TABLE_URI.getBytes());
  private static final String PRESIGNED_URL_PREFIX = "http://presigned-url/";

  private static final String CONTINUATION_TOKEN_PREFIX = "page_";

  @BeforeEach
  void setup() {
    mapper.registerModule(new JavaTimeModule());
    timelineCommitInstantsUploader =
        new TimelineCommitInstantsUploader(
            asyncStorageClient,
            presignedUrlFileUploader,
            onehouseApiClient,
            new StorageUtils(),
            ForkJoinPool.commonPool());
  }

  @Test
  void testUploadInstantsInArchivedTimeline() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(); // 1 file will be processed at a time
    // Page 1: returns 2 files
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/" + ARCHIVED_FOLDER_PREFIX,
        null,
        CONTINUATION_TOKEN_PREFIX + "1",
        List.of(
            generateFileObj("archived_instant_1", false),
            generateFileObj("archived_instant_2", false)));
    // page 3: returns 2 files (last page)
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/" + ARCHIVED_FOLDER_PREFIX,
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        List.of(
            generateFileObj("archived_instant_3", false),
            generateFileObj("archived_instant_4", false, currentTime)));
    Checkpoint checkpoint1 =
        generateCheckpointObj(1, Instant.EPOCH, false, null, "archived_instant_1");
    Checkpoint checkpoint2 =
        generateCheckpointObj(
            2, Instant.EPOCH, false, CONTINUATION_TOKEN_PREFIX + "1", "archived_instant_2");
    Checkpoint checkpoint3 =
        generateCheckpointObj(
            3, Instant.EPOCH, false, CONTINUATION_TOKEN_PREFIX + "1", "archived_instant_3");
    Checkpoint checkpoint4 =
        generateCheckpointObj(
            4,
            currentTime,
            true,
            null,
            "archived_instant_4"); // testing to makesure checkpoint timestamp has updated

    stubUploadInstantsCalls(
        List.of("archived_instant_1"),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    stubUploadInstantsCalls(
        List.of("archived_instant_2"),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    stubUploadInstantsCalls(
        List.of("archived_instant_3"),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    stubUploadInstantsCalls(
        List.of("archived_instant_4"),
        checkpoint4,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);

    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ARCHIVED_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
        .join();

    verify(asyncStorageClient, times(2)).fetchObjectsByPage(anyString(), anyString(), any());
    verifyFilesUploaded(
        List.of("archived_instant_1"),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        List.of("archived_instant_2"),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        List.of("archived_instant_3"),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        List.of("archived_instant_4"),
        checkpoint4,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
  }

  @Test
  void testUploadInstantsInActiveTimeline() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(); // 1 file will be processed at a time
    // Page 1: returns 2 files
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/",
        null,
        CONTINUATION_TOKEN_PREFIX + "1",
        List.of(
            generateFileObj("active_instant_1", false),
            generateFileObj("active_instant_2", false)));
    // page 3: returns 2 files (last page)
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/",
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        List.of(
            generateFileObj("active_instant_3", false),
            generateFileObj("active_instant_4", false, currentTime)));
    Checkpoint checkpoint1 =
        generateCheckpointObj(1, Instant.EPOCH, true, null, "active_instant_1");
    Checkpoint checkpoint2 =
        generateCheckpointObj(
            2, Instant.EPOCH, true, CONTINUATION_TOKEN_PREFIX + "1", "active_instant_2");
    Checkpoint checkpoint3 =
        generateCheckpointObj(
            3, Instant.EPOCH, true, CONTINUATION_TOKEN_PREFIX + "1", "active_instant_3");
    Checkpoint checkpoint4 =
        generateCheckpointObj(
            4,
            currentTime,
            true,
            null,
            "active_instant_4"); // testing to makesure checkpoint timestamp has updated

    stubUploadInstantsCalls(
        List.of("active_instant_1"), checkpoint1, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        List.of("active_instant_2"), checkpoint2, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        List.of("active_instant_3"), checkpoint3, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
        .join();

    verify(asyncStorageClient, times(2)).fetchObjectsByPage(anyString(), anyString(), any());
    verifyFilesUploaded(
        List.of("active_instant_1"), checkpoint1, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        List.of("active_instant_2"), checkpoint2, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        List.of("active_instant_3"), checkpoint3, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  @Test
  void testUploadInstantsInTimelineFromCheckpointNoContinuationToken() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(); // 1 file will be processed at a time
    // Page 1: returns 2 files
    // will still be listed as no continuation token is stored
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/",
        null,
        CONTINUATION_TOKEN_PREFIX + "1",
        List.of(
            generateFileObj("active_instant_1", false, currentTime.minus(15, ChronoUnit.SECONDS)),
            generateFileObj("active_instant_2", false, currentTime.minus(10, ChronoUnit.SECONDS))));
    // page 3: returns 2 files (last page)
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/",
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        List.of(
            generateFileObj("active_instant_3", false, currentTime.minus(5, ChronoUnit.SECONDS)),
            generateFileObj("active_instant_4", false, currentTime)));

    Checkpoint checkpoint4 =
        generateCheckpointObj(
            4,
            currentTime,
            true,
            null,
            "active_instant_4"); // testing to makesure checkpoint timestamp has updated

    // only active_instant_4 needs to be processed
    Checkpoint previousCheckpoint =
        generateCheckpointObj(
            3, currentTime.minus(5, ChronoUnit.SECONDS), true, null, "active_instant_3");

    stubUploadInstantsCalls(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, previousCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
        .join();

    verify(asyncStorageClient, times(2)).fetchObjectsByPage(anyString(), anyString(), any());
    verifyFilesUploaded(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  @Test
  void testUploadInstantsInTimelineFromCheckpointWithContinuationToken() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(); // 1 file will be processed at a time
    // Page 1: returns 2 files (SKIPPED)
    // page 3: returns 2 files (last page)
    mockListPage(
        TABLE.getRelativeTablePath() + "/.hoodie/",
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        List.of(
            generateFileObj("active_instant_3", false, currentTime.minus(5, ChronoUnit.SECONDS)),
            generateFileObj("active_instant_4", false, currentTime)));

    Checkpoint checkpoint4 =
        generateCheckpointObj(
            4,
            currentTime,
            true,
            null,
            "active_instant_4"); // testing to makesure checkpoint timestamp has updated

    // only active_instant_4 needs to be processed
    Checkpoint previousCheckpoint =
        generateCheckpointObj(
            3,
            currentTime.minus(5, ChronoUnit.SECONDS),
            true,
            CONTINUATION_TOKEN_PREFIX + "1",
            "active_instant_3");

    stubUploadInstantsCalls(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, previousCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
        .join();

    verify(asyncStorageClient, times(1)).fetchObjectsByPage(anyString(), anyString(), any());
    verifyFilesUploaded(
        List.of("active_instant_4"), checkpoint4, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  private void mockListPage(
      String prefix,
      String currentContinuationToken,
      String nextContinuationToken,
      List<File> files) {
    when(asyncStorageClient.fetchObjectsByPage("bucket", prefix, currentContinuationToken))
        .thenReturn(CompletableFuture.completedFuture(Pair.of(nextContinuationToken, files)));
  }

  private Checkpoint generateCheckpointObj(
      int batchId,
      Instant checkpointTimestamp,
      boolean archivedCommitsProcessed,
      String continuationToken,
      String lastUploadedFile) {
    return Checkpoint.builder()
        .batchId(batchId)
        .checkpointTimestamp(checkpointTimestamp)
        .archivedCommitsProcessed(archivedCommitsProcessed)
        .continuationToken(continuationToken)
        .lastUploadedFile(lastUploadedFile)
        .build();
  }

  private File generateFileObj(String fileName, boolean isDirectory) {
    return File.builder()
        .filename(fileName)
        .isDirectory(isDirectory)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }

  private File generateFileObj(String fileName, boolean isDirectory, Instant lastModifiedAt) {
    return File.builder()
        .filename(fileName)
        .isDirectory(isDirectory)
        .lastModifiedAt(lastModifiedAt)
        .build();
  }

  @SneakyThrows
  private void stubUploadInstantsCalls(
      List<String> filesUploaded,
      Checkpoint updatedCheckpoint,
      CommitTimelineType commitTimelineType) {
    List<String> filesUploadedWithUpdatedName =
        filesUploaded.stream()
            .map(fileName -> addPrefixToFileName(fileName, commitTimelineType))
            .collect(Collectors.toList());
    List<String> presignedUrls =
        filesUploadedWithUpdatedName.stream()
            .map(fileName -> PRESIGNED_URL_PREFIX + fileName)
            .collect(Collectors.toList());
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedWithUpdatedName)
                .commitTimelineType(commitTimelineType)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(presignedUrls)
                    .build()));
    for (String presignedUrl : presignedUrls) {
      String fileUri =
          S3_TABLE_URI + ".hoodie/" + presignedUrl.substring(PRESIGNED_URL_PREFIX.length());
      when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(presignedUrl), eq(fileUri)))
          .thenReturn(CompletableFuture.completedFuture(null));
    }
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(commitTimelineType)
                .tableId(TABLE_ID.toString())
                .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                .filesUploaded(filesUploadedWithUpdatedName)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));
  }

  @SneakyThrows
  private void verifyFilesUploaded(
      List<String> filesUploaded,
      Checkpoint updatedCheckpoint,
      CommitTimelineType commitTimelineType) {
    List<String> filesUploadedWithUpdatedName =
        filesUploaded.stream()
            .map(fileName -> addPrefixToFileName(fileName, commitTimelineType))
            .collect(Collectors.toList());
    List<String> presignedUrls =
        filesUploadedWithUpdatedName.stream()
            .map(fileName -> PRESIGNED_URL_PREFIX + fileName)
            .collect(Collectors.toList());
    verify(onehouseApiClient, times(1))
        .generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedWithUpdatedName)
                .commitTimelineType(commitTimelineType)
                .build());
    for (String presignedUrl : presignedUrls) {
      String fileUri =
          S3_TABLE_URI + ".hoodie/" + presignedUrl.substring(PRESIGNED_URL_PREFIX.length());
      verify(presignedUrlFileUploader, times(1))
          .uploadFileToPresignedUrl(eq(presignedUrl), eq(fileUri));
    }
    verify(onehouseApiClient, times(1))
        .upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(commitTimelineType)
                .tableId(TABLE_ID.toString())
                .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                .filesUploaded(filesUploadedWithUpdatedName)
                .build());
  }

  private String addPrefixToFileName(String fileName, CommitTimelineType commitTimelineType) {
    return (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
            ? ARCHIVED_FOLDER_PREFIX
            : "")
        + fileName;
  }
}
