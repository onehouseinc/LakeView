package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE_OBJ;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_CHECKPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.Arrays;
import java.util.Collections;
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
  @Mock private ActiveTimelineInstantBatcher activeTimelineInstantBatcher;
  private TimelineCommitInstantsUploader timelineCommitInstantsUploader;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final String S3_TABLE_URI = "s3://bucket/table/";
  private static final String ARCHIVED_FOLDER_PREFIX = "archived/";
  private static final Table TABLE =
      Table.builder()
          .absoluteTableUri(S3_TABLE_URI)
          .databaseName("database")
          .lakeName("lake")
          .build();
  private static final String TABLE_PREFIX = "table";
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
            ForkJoinPool.commonPool(),
            activeTimelineInstantBatcher);
  }

  @Test
  void testUploadInstantsInArchivedTimeline() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED); // 1 file will be processed at a time

    mockListAllFilesInDir(
        TABLE.getAbsoluteTableUri() + ".hoodie/" + ARCHIVED_FOLDER_PREFIX,
        Arrays.asList(
            generateFileObj("should_be_ignored", false),
            generateFileObj(".commits_.archive.1_1-0-1", false),
            generateFileObj(".commits_.archive.2_1-0-1", false),
            generateFileObj(".commits_.archive.3_1-0-1", false, currentTime)));

    Checkpoint checkpoint0 = generateCheckpointObj(1, Instant.EPOCH, false, HOODIE_PROPERTIES_FILE);
    Checkpoint checkpoint1 =
        generateCheckpointObj(2, Instant.EPOCH, false, ".commits_.archive.1_1-0-1");
    Checkpoint checkpoint2 =
        generateCheckpointObj(3, Instant.EPOCH, false, ".commits_.archive.2_1-0-1");
    Checkpoint checkpoint3 =
        generateCheckpointObj(
            4,
            currentTime,
            true,
            ".commits_.archive.3_1-0-1"); // testing to makesure checkpoint timestamp has updated

    stubUploadInstantsCalls(
        Collections.singletonList(HOODIE_PROPERTIES_FILE),
        checkpoint0,
        CommitTimelineType
            .COMMIT_TIMELINE_TYPE_ARCHIVED); // will be sent as part of archived timeline batch 1
    stubUploadInstantsCalls(
        Collections.singletonList(".commits_.archive.1_1-0-1"),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    stubUploadInstantsCalls(
        Collections.singletonList(".commits_.archive.2_1-0-1"),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    stubUploadInstantsCalls(
        Collections.singletonList(".commits_.archive.3_1-0-1"),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);

    // uploading instants in archived timeline for the first time
    Checkpoint response =
        timelineCommitInstantsUploaderSpy
            .batchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                INITIAL_CHECKPOINT,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
            .join();

    verify(asyncStorageClient, times(1)).listAllFilesInDir(anyString());
    verifyFilesUploaded(
        Collections.singletonList(HOODIE_PROPERTIES_FILE),
        checkpoint0,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        Collections.singletonList(".commits_.archive.1_1-0-1"),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        Collections.singletonList(".commits_.archive.2_1-0-1"),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verifyFilesUploaded(
        Collections.singletonList(".commits_.archive.3_1-0-1"),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    assertEquals(checkpoint3, response);
  }

  @Test
  void testUploadInstantsInActiveTimelineArchivedTimelineNotPresent() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(4)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE); // 1 file will be processed at a time
    // Page 1: returns 2 files
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        Arrays.asList(
            generateFileObj("should_be_ignored", false),
            generateFileObj("111.action", false),
            generateFileObj("111.action.inflight", false),
            generateFileObj("111.action.requested", false),
            generateFileObj("222.action", false, currentTime)));
    // page 2: returns 2 files (last page)
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        null,
        TABLE_PREFIX
            + "/.hoodie/"
            + "111.action", // last successful commit is used for checkpointing
        Arrays.asList(
            generateFileObj("111.action.inflight", false),
            generateFileObj("111.action.requested", false),
            generateFileObj("222.action", false, currentTime),
            generateFileObj("222.action.inflight", false),
            generateFileObj("222.action.requested", false),
            generateFileObj(HOODIE_PROPERTIES_FILE, false) // will be listed
            ));

    List<File> batch1 =
        Arrays.asList(
            generateFileObj(HOODIE_PROPERTIES_FILE, false),
            generateFileObj("111.action", false),
            generateFileObj("111.action.inflight", false),
            generateFileObj("111.action.requested", false));

    List<File> batch2 =
        Arrays.asList(
            generateFileObj("222.action", false, currentTime),
            generateFileObj("222.action.inflight", false),
            generateFileObj("222.action.requested", false));

    stubCreateBatches(
        Arrays.asList(
            generateFileObj(HOODIE_PROPERTIES_FILE, false),
            generateFileObj("111.action", false),
            generateFileObj("111.action.inflight", false),
            generateFileObj("111.action.requested", false),
            generateFileObj("222.action", false, currentTime)),
        Collections.singletonList(batch1));

    stubCreateBatches(
        Arrays.asList(
            generateFileObj("222.action", false, currentTime),
            generateFileObj("222.action.inflight", false),
            generateFileObj("222.action.requested", false)),
        Collections.singletonList(batch2));

    Checkpoint checkpoint1 = generateCheckpointObj(1, Instant.EPOCH, true, "111.action");
    Checkpoint checkpoint2 = generateCheckpointObj(2, currentTime, true, "222.action");

    stubUploadInstantsCalls(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        batch2.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    Checkpoint response =
        timelineCommitInstantsUploaderSpy
            .paginatedBatchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                INITIAL_CHECKPOINT,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
            .join();

    verify(asyncStorageClient, times(2)).fetchObjectsByPage(anyString(), anyString(), any(), any());
    verifyFilesUploaded(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        batch2.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    assertEquals(checkpoint2, response);
  }

  @Test
  void testUploadInstantsInEmptyActiveTimelineWhenArchivedTimelineNotPresent() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);

    doReturn(4)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE); // 1 file will be processed at a time
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        null,
        null,
        Collections.singletonList(HOODIE_PROPERTIES_FILE_OBJ));

    List<File> batch1 = Collections.singletonList(generateFileObj(HOODIE_PROPERTIES_FILE, false));

    stubCreateBatches(
        Collections.singletonList(generateFileObj(HOODIE_PROPERTIES_FILE, false)),
        Collections.singletonList(batch1));

    Checkpoint checkpoint1 = generateCheckpointObj(1, Instant.EPOCH, true, HOODIE_PROPERTIES_FILE);

    stubUploadInstantsCalls(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    Checkpoint response =
        timelineCommitInstantsUploaderSpy
            .paginatedBatchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                INITIAL_CHECKPOINT,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
            .join();

    verify(asyncStorageClient, times(1)).fetchObjectsByPage(anyString(), anyString(), any(), any());
    verifyFilesUploaded(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    assertEquals(checkpoint1, response);
  }

  @Test
  void testUploadInstantsInActiveTimelineWithOnlySavepoint() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(4)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE); // 1 file will be processed at a time
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        null,
        null,
        Arrays.asList(
            generateFileObj("222.savepoint.inflight", false),
            generateFileObj("222.savepoint", false, currentTime),
            generateFileObj(HOODIE_PROPERTIES_FILE, false)));

    List<File> batch1 =
        Arrays.asList(
            generateFileObj(HOODIE_PROPERTIES_FILE, false),
            generateFileObj("222.savepoint", false, currentTime),
            generateFileObj("222.savepoint.inflight", false));

    stubCreateBatches(
        Arrays.asList(
            generateFileObj(HOODIE_PROPERTIES_FILE, false),
            generateFileObj("222.savepoint", false, currentTime),
            generateFileObj("222.savepoint.inflight", false)),
        Collections.singletonList(batch1));

    Checkpoint checkpoint1 = generateCheckpointObj(1, currentTime, true, "222.savepoint");

    stubUploadInstantsCalls(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    Checkpoint response =
        timelineCommitInstantsUploaderSpy
            .paginatedBatchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                INITIAL_CHECKPOINT,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
            .join();

    verify(asyncStorageClient, times(1)).fetchObjectsByPage(anyString(), anyString(), any(), any());
    verifyFilesUploaded(
        batch1.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    assertEquals(checkpoint1, response);
  }

  @Test
  void testUploadInstantsInActiveTimelineFromCheckpoint() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    Instant currentTime = Instant.now();

    doReturn(4)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE); // 1 file will be processed at a time
    // Page 1: returns 2 files (SKIPPED)
    // page 2: returns 2 files (last page)

    // only active_instant_4 needs to be processed
    Checkpoint previousCheckpoint =
        generateCheckpointObj(2, currentTime.minus(10, ChronoUnit.SECONDS), true, "222.action");

    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        null,
        TABLE_PREFIX + "/.hoodie/" + previousCheckpoint.getLastUploadedFile(),
        Arrays.asList(
            generateFileObj("222.action.inflight", false),
            generateFileObj("222.action.requested", false),
            generateFileObj("333.action", false, currentTime),
            generateFileObj("333.action.inflight", false),
            generateFileObj("333.action.requested", false),
            generateFileObj(HOODIE_PROPERTIES_FILE, false)));

    Checkpoint checkpoint3 =
        generateCheckpointObj(
            3,
            currentTime, // testing to makesure checkpoint timestamp has updated
            true,
            "333.action");

    List<File> batch3 =
        Arrays.asList(
            generateFileObj("333.action", false, currentTime),
            generateFileObj("333.action.inflight", false),
            generateFileObj("333.action.requested", false));

    stubCreateBatches(
        Arrays.asList(
            generateFileObj("333.action", false, currentTime),
            generateFileObj("333.action.inflight", false),
            generateFileObj("333.action.requested", false)),
        Collections.singletonList(batch3));

    stubUploadInstantsCalls(
        batch3.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    timelineCommitInstantsUploaderSpy
        .paginatedBatchUploadWithCheckpoint(
            TABLE_ID.toString(),
            TABLE,
            previousCheckpoint,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
        .join();

    verify(asyncStorageClient, times(1)).fetchObjectsByPage(anyString(), anyString(), any(), any());
    verifyFilesUploaded(
        batch3.stream().map(File::getFilename).collect(Collectors.toList()),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  @Test
  void testUploadInstantsInArchivedTimelineWhenNoInstantsPresent() {
    // no files present in archived timeline
    mockListAllFilesInDir(
        TABLE.getAbsoluteTableUri() + ".hoodie/" + ARCHIVED_FOLDER_PREFIX, Collections.emptyList());

    // uploading instants in archived timeline for the first time
    Checkpoint checkpoint =
        timelineCommitInstantsUploader
            .batchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                INITIAL_CHECKPOINT,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
            .join();

    verify(asyncStorageClient, times(1)).listAllFilesInDir(anyString());
    assertEquals(INITIAL_CHECKPOINT, checkpoint);
  }

  @Test
  void testUploadInstantFailureWhenGeneratingUploadUrl() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED); // 1 file will be processed at a time

    mockListAllFilesInDir(
        TABLE.getAbsoluteTableUri() + ".hoodie/" + ARCHIVED_FOLDER_PREFIX,
        Arrays.asList(
            generateFileObj(".commits_.archive.1_1-0-1", false),
            generateFileObj(".commits_.archive.2_1-0-1", false)));

    List<String> filesUploadedWithUpdatedName = Collections.singletonList("hoodie.properties");
    GenerateCommitMetadataUploadUrlRequest expectedRequest =
        GenerateCommitMetadataUploadUrlRequest.builder()
            .tableId(TABLE_ID.toString())
            .commitInstants(filesUploadedWithUpdatedName)
            .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
            .build();
    GenerateCommitMetadataUploadUrlResponse failureResponse =
        GenerateCommitMetadataUploadUrlResponse.builder().build();
    failureResponse.setError(500, "api error");
    when(onehouseApiClient.generateCommitMetadataUploadUrl(expectedRequest))
        .thenReturn(CompletableFuture.completedFuture(failureResponse));

    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .batchUploadWithCheckpoint(
            TABLE_ID.toString(),
            TABLE,
            INITIAL_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
        .join();

    // generate commit metadata api call will fail and no more batches will be processed
    verify(asyncStorageClient, times(1)).listAllFilesInDir(anyString());
    verify(onehouseApiClient, times(1)).generateCommitMetadataUploadUrl(expectedRequest);
    verify(presignedUrlFileUploader, times(0)).uploadFileToPresignedUrl(any(), any());
  }

  @Test
  @SneakyThrows
  void testUploadInstantFailureWhenUpdatingCheckpoint() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);

    doReturn(1)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED); // 1 file will be processed at a time
    mockListAllFilesInDir(
        TABLE.getAbsoluteTableUri() + ".hoodie/" + ARCHIVED_FOLDER_PREFIX,
        Arrays.asList(
            generateFileObj(".commits_.archive.1_1-0-1", false),
            generateFileObj(".commits_.archive.2_1-0-1", false)));

    Checkpoint checkpoint0 = generateCheckpointObj(1, Instant.EPOCH, false, HOODIE_PROPERTIES_FILE);

    List<String> filesUploadedWithUpdatedName = Collections.singletonList("hoodie.properties");
    GenerateCommitMetadataUploadUrlRequest expectedRequest =
        GenerateCommitMetadataUploadUrlRequest.builder()
            .tableId(TABLE_ID.toString())
            .commitInstants(filesUploadedWithUpdatedName)
            .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
            .build();
    UpsertTableMetricsCheckpointResponse failureResponse =
        UpsertTableMetricsCheckpointResponse.builder().build();
    failureResponse.setError(500, "api error");
    when(onehouseApiClient.generateCommitMetadataUploadUrl(expectedRequest))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(
                        filesUploadedWithUpdatedName.stream()
                            .map(file -> PRESIGNED_URL_PREFIX + file)
                            .collect(Collectors.toList()))
                    .build()));
    when(presignedUrlFileUploader.uploadFileToPresignedUrl(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .tableId(TABLE_ID.toString())
                .checkpoint(mapper.writeValueAsString(checkpoint0))
                .filesUploaded(filesUploadedWithUpdatedName)
                .build()))
        .thenReturn(CompletableFuture.completedFuture(failureResponse));
    // uploading instants in archived timeline for the first time
    timelineCommitInstantsUploaderSpy
        .batchUploadWithCheckpoint(
            TABLE_ID.toString(),
            TABLE,
            INITIAL_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
        .join();

    // update checkpoint api call will fail and no more batches will be processed
    verify(asyncStorageClient, times(1)).listAllFilesInDir(anyString());
    verify(onehouseApiClient, times(1)).generateCommitMetadataUploadUrl(expectedRequest);
    verify(presignedUrlFileUploader, times(1)).uploadFileToPresignedUrl(any(), any());
    verify(onehouseApiClient, times(1)).upsertTableMetricsCheckpoint(any());
  }

  @Test
  void testGetUploadBatchSize() {
    assertEquals(
        20,
        timelineCommitInstantsUploader.getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE));
    assertEquals(
        2,
        timelineCommitInstantsUploader.getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED));
  }

  private void mockListPage(
      String prefix, String nextContinuationToken, String startAfter, List<File> files) {
    when(asyncStorageClient.fetchObjectsByPage("bucket", prefix, null, startAfter))
        .thenReturn(CompletableFuture.completedFuture(Pair.of(nextContinuationToken, files)));
  }

  private void mockListAllFilesInDir(String dirUri, List<File> files) {
    when(asyncStorageClient.listAllFilesInDir(dirUri))
        .thenReturn(CompletableFuture.completedFuture(files));
  }

  private Checkpoint generateCheckpointObj(
      int batchId,
      Instant checkpointTimestamp,
      boolean archivedCommitsProcessed,
      String lastUploadedFile) {
    return Checkpoint.builder()
        .batchId(batchId)
        .checkpointTimestamp(checkpointTimestamp)
        .archivedCommitsProcessed(archivedCommitsProcessed)
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
      when(presignedUrlFileUploader.uploadFileToPresignedUrl(presignedUrl, fileUri))
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

  private void stubCreateBatches(List<File> files, List<List<File>> expectedBatches) {
    when(activeTimelineInstantBatcher.createBatches(files, 4)).thenReturn(expectedBatches);
  }

  private String addPrefixToFileName(String fileName, CommitTimelineType commitTimelineType) {
    return (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
                && !HOODIE_PROPERTIES_FILE.equals(fileName)
            ? ARCHIVED_FOLDER_PREFIX
            : "")
        + fileName;
  }
}
