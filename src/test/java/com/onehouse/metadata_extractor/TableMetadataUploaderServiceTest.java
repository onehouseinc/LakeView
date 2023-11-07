package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.models.request.TableType;
import com.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.PresignedUrlFileUploader;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableMetadataUploaderServiceTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  @Mock private HoodiePropertiesReader hoodiePropertiesReader;
  @Mock private PresignedUrlFileUploader presignedUrlFileUploader;
  @Mock private OnehouseApiClient onehouseApiClient;
  private TableMetadataUploaderService tableMetadataUploaderService;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final String S3_TABLE_URI = "s3://bucket/table/";
  private static final Table TABLE =
      Table.builder()
          .absoluteTableUri(S3_TABLE_URI)
          .relativeTablePath("table")
          .databaseName("database")
          .lakeName("lake")
          .build();
  ;
  private static final UUID TABLE_ID = UUID.nameUUIDFromBytes(S3_TABLE_URI.getBytes());
  private static final ParsedHudiProperties PARSED_HUDI_PROPERTIES =
      ParsedHudiProperties.builder()
          .tableName("tableName")
          .tableType(TableType.COPY_ON_WRITE)
          .build();
  private static final String PRESIGNED_URL = "http://presigned-url";

  @BeforeEach
  void setup() {
    mapper.registerModule(new JavaTimeModule());
    tableMetadataUploaderService =
        new TableMetadataUploaderService(
            asyncStorageClient,
            hoodiePropertiesReader,
            presignedUrlFileUploader,
            new StorageUtils(),
            onehouseApiClient,
            ForkJoinPool.commonPool());
  }

  @Test
  @SneakyThrows
  void testUploadMetadataOfANewlyDiscoveredTable() {
    filesInActiveTimelineMock(
        List.of(
            generateFileObj("instant1", false),
            generateFileObj("hoodie.properties", false),
            generateFileObj("archived", true),
            generateFileObj("some-other-folder-1", true)));
    filesInArchivedTimelineMock(
        List.of(
            generateFileObj("archived_instant1", false),
            generateFileObj("some-other-folder-2", true)));

    GetTableMetricsCheckpointResponse mockedResponse =
        GetTableMetricsCheckpointResponse.builder().build();
    mockedResponse.setError(404, "");

    InitializeTableMetricsCheckpointResponse initializeTableMetricsCheckpointResponse =
        InitializeTableMetricsCheckpointResponse.builder().build();

    String archivedTimelineCheckpoint =
        mapper.writeValueAsString(
            generateCheckpointObj(1, Instant.EPOCH, true, "archived_instant1"));

    String activeTimelineCheckpoint =
        mapper.writeValueAsString(generateCheckpointObj(1, Instant.EPOCH, true, "instant1"));

    List<String> filesUploadedFromArchivedTimeline = List.of("archived_instant1");
    List<String> filesUploadedFromActiveTimeline = List.of("hoodie.properties", "instant1");

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(CompletableFuture.completedFuture(mockedResponse));
    when(hoodiePropertiesReader.readHoodieProperties(
            String.format("%s%s/%s", S3_TABLE_URI, HOODIE_FOLDER_NAME, HOODIE_PROPERTIES_FILE)))
        .thenReturn(CompletableFuture.completedFuture(PARSED_HUDI_PROPERTIES));
    when(onehouseApiClient.initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tableId(TABLE_ID)
                .tableName(PARSED_HUDI_PROPERTIES.getTableName())
                .tableType(PARSED_HUDI_PROPERTIES.getTableType())
                .tableBasePath(TABLE.getRelativeTablePath())
                .databaseName(TABLE.getDatabaseName())
                .lakeName(TABLE.getLakeName())
                .build()))
        .thenReturn(CompletableFuture.completedFuture(initializeTableMetricsCheckpointResponse));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedFromActiveTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(PRESIGNED_URL, PRESIGNED_URL))
                    .build()));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedFromArchivedTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(PRESIGNED_URL))
                    .build()));
    when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(PRESIGNED_URL), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .tableId(TABLE_ID.toString())
                .checkpoint(archivedTimelineCheckpoint)
                .filesUploaded(filesUploadedFromArchivedTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .tableId(TABLE_ID.toString())
                .checkpoint(activeTimelineCheckpoint)
                .filesUploaded(filesUploadedFromActiveTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // three files (2 from active and 1 from archived need to be uploaded)
    verify(presignedUrlFileUploader, times(3)).uploadFileToPresignedUrl(eq(PRESIGNED_URL), any());
  }

  @Test
  @SneakyThrows
  void testUploadMetadataOfATableWithoutArchived() {
    filesInActiveTimelineMock(
        List.of(
            generateFileObj("instant1", false),
            generateFileObj("hoodie.properties", false),
            generateFileObj("archived", true),
            generateFileObj("some-other-folder-1", true)));

    filesInArchivedTimelineMock(List.of());

    GetTableMetricsCheckpointResponse mockedResponse =
        GetTableMetricsCheckpointResponse.builder().build();
    mockedResponse.setError(404, "");

    InitializeTableMetricsCheckpointResponse initializeTableMetricsCheckpointResponse =
        InitializeTableMetricsCheckpointResponse.builder().build();

    String activeTimelineCheckpoint =
        mapper.writeValueAsString(generateCheckpointObj(1, Instant.EPOCH, true, "instant1"));

    List<String> filesUploadedFromActiveTimeline = List.of("hoodie.properties", "instant1");

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(CompletableFuture.completedFuture(mockedResponse));
    when(hoodiePropertiesReader.readHoodieProperties(
            String.format("%s%s/%s", S3_TABLE_URI, HOODIE_FOLDER_NAME, HOODIE_PROPERTIES_FILE)))
        .thenReturn(CompletableFuture.completedFuture(PARSED_HUDI_PROPERTIES));
    when(onehouseApiClient.initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tableId(TABLE_ID)
                .tableName(PARSED_HUDI_PROPERTIES.getTableName())
                .tableType(PARSED_HUDI_PROPERTIES.getTableType())
                .tableBasePath(TABLE.getRelativeTablePath())
                .databaseName(TABLE.getDatabaseName())
                .lakeName(TABLE.getLakeName())
                .build()))
        .thenReturn(CompletableFuture.completedFuture(initializeTableMetricsCheckpointResponse));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedFromActiveTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(PRESIGNED_URL, PRESIGNED_URL))
                    .build()));
    when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(PRESIGNED_URL), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .tableId(TABLE_ID.toString())
                .checkpoint(activeTimelineCheckpoint)
                .filesUploaded(filesUploadedFromActiveTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // two files (both from active)
    verify(presignedUrlFileUploader, times(2)).uploadFileToPresignedUrl(eq(PRESIGNED_URL), any());
  }

  @Test
  @SneakyThrows
  void testUploadMetadataFromPreviousArchivedCheckpoint() {
    filesInActiveTimelineMock(
        List.of(
            generateFileObj("instant1", false),
            generateFileObj("hoodie.properties", false),
            generateFileObj("archived", true),
            generateFileObj("some-other-folder-1", true)));

    filesInArchivedTimelineMock(
        List.of(
            generateFileObj("archived_instant1", false),
            generateFileObj("archived_instant2", false, Instant.EPOCH.plus(5, ChronoUnit.MINUTES)),
            generateFileObj("some-other-folder-2", true)));

    // archived_instant1 has already been uploaded in previous run
    String CurrentCheckpoint =
        mapper.writeValueAsString(
            generateCheckpointObj(1, Instant.EPOCH, false, "archived_instant1"));

    String archivedTimelineCheckpoint =
        mapper.writeValueAsString(
            generateCheckpointObj(
                2, Instant.EPOCH.plus(5, ChronoUnit.MINUTES), true, "archived_instant2"));

    String activeTimelineCheckpoint =
        mapper.writeValueAsString(generateCheckpointObj(1, Instant.EPOCH, true, "instant1"));

    List<String> filesUploadedFromArchivedTimeline = List.of("archived_instant2");

    List<String> filesUploadedFromActiveTimeline = List.of("hoodie.properties", "instant1");

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder().checkpoint(CurrentCheckpoint).build()));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedFromActiveTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(PRESIGNED_URL, PRESIGNED_URL))
                    .build()));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(TABLE_ID.toString())
                .commitInstants(filesUploadedFromArchivedTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(PRESIGNED_URL))
                    .build()));
    when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(PRESIGNED_URL), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .tableId(TABLE_ID.toString())
                .checkpoint(archivedTimelineCheckpoint)
                .filesUploaded(filesUploadedFromArchivedTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .tableId(TABLE_ID.toString())
                .checkpoint(activeTimelineCheckpoint)
                .filesUploaded(filesUploadedFromActiveTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // three files (2 from active and 1 from archived need to be uploaded)
    verify(presignedUrlFileUploader, times(3)).uploadFileToPresignedUrl(eq(PRESIGNED_URL), any());
  }

  @Test
  @SneakyThrows
  void testUploadMetadataFromPreviousArchivedCheckpointNoMoreArchivedInstantsToProcess() {
    filesInActiveTimelineMock(
            List.of(
                    generateFileObj("instant1", false),
                    generateFileObj("hoodie.properties", false),
                    generateFileObj("archived", true),
                    generateFileObj("some-other-folder-1", true)));

    // list files in archived timeline will not be called as archived timeline is already processed
    String CurrentCheckpoint =
            mapper.writeValueAsString(
                    generateCheckpointObj(1, Instant.EPOCH, true, "archived_instant1"));


    String expectedActiveTimelineCheckpoint =
            mapper.writeValueAsString(generateCheckpointObj(1, Instant.EPOCH, true, "instant1"));

    List<String> filesUploadedFromActiveTimeline = List.of("hoodie.properties", "instant1");

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
            .thenReturn(
                    CompletableFuture.completedFuture(
                            GetTableMetricsCheckpointResponse.builder().checkpoint(CurrentCheckpoint).build()));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                    .tableId(TABLE_ID.toString())
                    .commitInstants(filesUploadedFromActiveTimeline)
                    .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                    .build()))
            .thenReturn(
                    CompletableFuture.completedFuture(
                            GenerateCommitMetadataUploadUrlResponse.builder()
                                    .uploadUrls(List.of(PRESIGNED_URL, PRESIGNED_URL))
                                    .build()));

    when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(PRESIGNED_URL), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                    .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                    .tableId(TABLE_ID.toString())
                    .checkpoint(expectedActiveTimelineCheckpoint)
                    .filesUploaded(filesUploadedFromActiveTimeline)
                    .build()))
            .thenReturn(
                    CompletableFuture.completedFuture(
                            UpsertTableMetricsCheckpointResponse.builder().build()));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // just the two files in active timeline need to be uploaded
    verify(presignedUrlFileUploader, times(2)).uploadFileToPresignedUrl(eq(PRESIGNED_URL), any());
  }

  private void filesInActiveTimelineMock(List<File> files) {
    // stub list files in active-timeline
    when(asyncStorageClient.listAllFilesInDir(S3_TABLE_URI + HOODIE_FOLDER_NAME + "/"))
        .thenReturn(CompletableFuture.completedFuture(files));
  }

  private void filesInArchivedTimelineMock(List<File> files) {
    // stub list files in archived-timeline
    when(asyncStorageClient.listAllFilesInDir(
            S3_TABLE_URI + String.format("%s/%s/", HOODIE_FOLDER_NAME, ARCHIVED_FOLDER_NAME)))
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
}
