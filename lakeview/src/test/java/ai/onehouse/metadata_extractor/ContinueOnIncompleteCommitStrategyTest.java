package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.onehouse.api.models.request.UploadedFile;
import com.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.config.Config;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.metrics.LakeViewExtractorMetrics;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.PresignedUrlFileUploader;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ContinueOnIncompleteCommitStrategyTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  @Mock private PresignedUrlFileUploader presignedUrlFileUploader;
  @Mock private OnehouseApiClient onehouseApiClient;
  @Mock private Config config;
  @Mock private MetadataExtractorConfig metadataExtractorConfig;
  @Mock private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
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

  private TimelineCommitInstantsUploader getTimelineCommitInstantsUploader(TestInfo testInfo) {
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getUploadStrategy())
        .thenReturn(MetadataExtractorConfig.UploadStrategy.CONTINUE_ON_INCOMPLETE_COMMIT);
    ActiveTimelineInstantBatcher activeTimelineInstantBatcher =
        new ActiveTimelineInstantBatcher(config);
    return new TimelineCommitInstantsUploader(
        asyncStorageClient,
        presignedUrlFileUploader,
        onehouseApiClient,
        new StorageUtils(),
        ForkJoinPool.commonPool(),
        activeTimelineInstantBatcher,
        hudiMetadataExtractorMetrics,
        config);
  }

  @BeforeEach
  void setup(TestInfo testInfo) {
    mapper.registerModule(new JavaTimeModule());
    timelineCommitInstantsUploader = getTimelineCommitInstantsUploader(testInfo);
  }

  @Test
  void testContinueOnIncompleteCommitApproach() {
    TimelineCommitInstantsUploader timelineCommitInstantsUploaderSpy =
        spy(timelineCommitInstantsUploader);
    doReturn(4)
        .when(timelineCommitInstantsUploaderSpy)
        .getUploadBatchSize(
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE); // 1 file will be processed at a time
    Checkpoint previousCheckpoint = getCheckpoint("01-01-1970", 3, "", "");
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        CONTINUATION_TOKEN_PREFIX + "1",
        null,
        Arrays.asList(
            generateFileObj("111.deltacommit.requested"),
            generateFileObj("111.deltacommit.inflight"),
            generateFileObj("111.deltacommit"),
            generateFileObj("333.clean"),
            generateFileObj("444.rollback.requested"), // Incomplete commit
            generateFileObj("333.clean.requested"),
            generateFileObj(
                "222.clean.inflight",
                "21-07-2024"), // incomplete commit skipped but not completed in subsequent run
            generateFileObj("333.clean.inflight"),
            generateFileObj("222.clean.requested", "21-07-2024"),
            generateFileObj("444.rollback.inflight"),
            generateFileObj("666.rollback.requested"), // Incomplete commit
            generateFileObj("777.rollback.requested"),
            generateFileObj("777.rollback.inflight"),
            generateFileObj("777.rollback")));
    mockListPage(
        TABLE_PREFIX + "/.hoodie/",
        null,
        "table/.hoodie/777.rollback", // last successful commit is used for checkpointing
        new ArrayList<>());

    List<File> batch1 =
        Stream.of(
                generateFileObj("111.deltacommit"),
                generateFileObj("111.deltacommit.inflight"),
                generateFileObj("111.deltacommit.requested"))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    List<File> batch2 =
        Arrays.asList(
            generateFileObj("333.clean"),
            generateFileObj("333.clean.inflight"),
            generateFileObj("333.clean.requested"));
    List<File> batch3 =
        Arrays.asList(
            generateFileObj("777.rollback"),
            generateFileObj("777.rollback.inflight"),
            generateFileObj("777.rollback.requested"));

    Checkpoint checkpoint1 = getCheckpoint("23-07-2024", 4, "111.deltacommit", "221");
    Checkpoint checkpoint2 = getCheckpoint("23-07-2024", 5, "333.clean", "221");
    Checkpoint checkpoint3 = getCheckpoint("23-07-2024", 6, "777.rollback", "221");

    stubUploadInstantsCalls(
        batch1.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        batch2.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    stubUploadInstantsCalls(
        batch3.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);

    Checkpoint response =
        timelineCommitInstantsUploaderSpy
            .paginatedBatchUploadWithCheckpoint(
                TABLE_ID.toString(),
                TABLE,
                previousCheckpoint,
                CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
            .join();

    verify(asyncStorageClient, times(2)).fetchObjectsByPage(anyString(), anyString(), any(), any());
    verifyFilesUploaded(
        batch1.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        batch2.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint2,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        batch1.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint1,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    verifyFilesUploaded(
        batch3.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(file.getFilename())
                        .lastModifiedAt(file.getLastModifiedAt().toEpochMilli())
                        .build())
            .collect(Collectors.toList()),
        checkpoint3,
        CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
    assertEquals(checkpoint3, response);
  }

  private void mockListPage(
      String prefix, String nextContinuationToken, String startAfter, List<File> files) {
    when(asyncStorageClient.fetchObjectsByPage("bucket", prefix, null, startAfter))
        .thenReturn(CompletableFuture.completedFuture(Pair.of(nextContinuationToken, files)));
  }

  static File generateFileObj(String fileName) {
    return generateFileObj(fileName, "23-07-2024");
  }

  static File generateFileObj(String fileName, String dateString) {
    Instant instant =
        LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
            .atStartOfDay()
            .toInstant(java.time.ZoneOffset.UTC);
    return File.builder().filename(fileName).isDirectory(false).lastModifiedAt(instant).build();
  }

  @SneakyThrows
  private void stubUploadInstantsCalls(
      List<UploadedFile> filesUploaded,
      Checkpoint updatedCheckpoint,
      CommitTimelineType commitTimelineType) {
    filesUploaded =
        filesUploaded.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(addPrefixToFileName(file.getName(), commitTimelineType))
                        .lastModifiedAt(file.getLastModifiedAt())
                        .build())
            .collect(Collectors.toList());
    List<String> filesUploadedWithUpdatedName =
        filesUploaded.stream().map(UploadedFile::getName).collect(Collectors.toList());
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
      when(presignedUrlFileUploader.uploadFileToPresignedUrl(
              presignedUrl, fileUri, metadataExtractorConfig.getFileUploadStreamBatchSize()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(commitTimelineType)
                .tableId(TABLE_ID.toString())
                .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                .filesUploaded(filesUploadedWithUpdatedName)
                .uploadedFiles(filesUploaded)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));
  }

  @SneakyThrows
  private void verifyFilesUploaded(
      List<UploadedFile> filesUploaded,
      Checkpoint updatedCheckpoint,
      CommitTimelineType commitTimelineType) {
    filesUploaded =
        filesUploaded.stream()
            .map(
                file ->
                    UploadedFile.builder()
                        .name(addPrefixToFileName(file.getName(), commitTimelineType))
                        .lastModifiedAt(file.getLastModifiedAt())
                        .build())
            .collect(Collectors.toList());
    List<String> filesUploadedWithUpdatedName =
        filesUploaded.stream().map(UploadedFile::getName).collect(Collectors.toList());
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
          .uploadFileToPresignedUrl(
              presignedUrl, fileUri, metadataExtractorConfig.getFileUploadStreamBatchSize());
    }
    verify(onehouseApiClient, times(1))
        .upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(commitTimelineType)
                .tableId(TABLE_ID.toString())
                .checkpoint(mapper.writeValueAsString(updatedCheckpoint))
                .filesUploaded(filesUploadedWithUpdatedName)
                .uploadedFiles(filesUploaded)
                .build());
  }

  private String addPrefixToFileName(String fileName, CommitTimelineType commitTimelineType) {
    return (CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED.equals(commitTimelineType)
                && !HOODIE_PROPERTIES_FILE.equals(fileName)
            ? ARCHIVED_FOLDER_PREFIX
            : "")
        + fileName;
  }

  static Checkpoint getCheckpoint(
      String dateString,
      Integer batchId,
      String lastUploadedFile,
      String firstIncompleteCheckpoint) {
    Instant instant =
        LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
            .atStartOfDay()
            .toInstant(java.time.ZoneOffset.UTC);
    return Checkpoint.builder()
        .checkpointTimestamp(instant)
        .batchId(batchId)
        .lastUploadedFile(lastUploadedFile)
        .firstIncompleteCheckpoint(firstIncompleteCheckpoint)
        .archivedCommitsProcessed(true)
        .build();
  }
}
