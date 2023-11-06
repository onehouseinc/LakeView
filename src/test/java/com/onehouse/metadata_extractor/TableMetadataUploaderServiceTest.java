package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.ARCHIVED_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private ObjectMapper mapper = new ObjectMapper();

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
    String s3TableUri = "s3://bucket/table/";
    UUID tableId = UUID.nameUUIDFromBytes(s3TableUri.getBytes());
    Table table =
        Table.builder()
            .absoluteTableUri(s3TableUri)
            .relativeTablePath("table")
            .databaseName("database")
            .lakeName("lake")
            .build();
    ParsedHudiProperties parsedHudiProperties =
        ParsedHudiProperties.builder()
            .tableName("tableName")
            .tableType(TableType.COPY_ON_WRITE)
            .build();
    String presignedUrl = "http://presigned-url";

    // stub list files in active-timeline
    when(asyncStorageClient.listAllFilesInDir(s3TableUri + HOODIE_FOLDER_NAME + "/"))
        .thenReturn(
            CompletableFuture.completedFuture(
                List.of(
                    generateFileObj("instant1", false),
                    generateFileObj("hoodie.properties", false),
                    generateFileObj("archived", true),
                    generateFileObj("some-other-folder-1", true))));

    // stub list files in archived-timeline
    when(asyncStorageClient.listAllFilesInDir(
            s3TableUri + String.format("%s/%s/", HOODIE_FOLDER_NAME, ARCHIVED_FOLDER_NAME)))
        .thenReturn(
            CompletableFuture.completedFuture(
                List.of(
                    generateFileObj("archived_instant1", false),
                    generateFileObj("some-other-folder-2", true))));

    GetTableMetricsCheckpointResponse mockedResponse =
        GetTableMetricsCheckpointResponse.builder().build();
    InitializeTableMetricsCheckpointResponse initializeTableMetricsCheckpointResponse =
        InitializeTableMetricsCheckpointResponse.builder().build();
    String archivedTimelineCheckpoint =
        mapper.writeValueAsString(
            Checkpoint.builder()
                .batchId(1)
                .lastUploadedFile("archived_instant1")
                .checkpointTimestamp(Instant.EPOCH)
                .archivedCommitsProcessed(true)
                .build());
    String activeTimelineCheckpoint =
        mapper.writeValueAsString(
            Checkpoint.builder()
                .batchId(1)
                .lastUploadedFile("instant1")
                .checkpointTimestamp(Instant.EPOCH)
                .archivedCommitsProcessed(true)
                .build());

    List<String> filesUploadedFromArchivedTimeline =
        Stream.of(generateFileObj("archived_instant1", false))
            .map(File::getFilename)
            .collect(Collectors.toList());
    List<String> filesUploadedFromActiveTimeline =
        Stream.of(generateFileObj("hoodie.properties", false), generateFileObj("instant1", false))
            .map(File::getFilename)
            .collect(Collectors.toList());

    mockedResponse.setError(404, "");
    when(onehouseApiClient.getTableMetricsCheckpoint(tableId.toString()))
        .thenReturn(CompletableFuture.completedFuture(mockedResponse));
    when(hoodiePropertiesReader.readHoodieProperties(
            String.format("%s%s/%s", s3TableUri, HOODIE_FOLDER_NAME, HOODIE_PROPERTIES_FILE)))
        .thenReturn(CompletableFuture.completedFuture(parsedHudiProperties));
    when(onehouseApiClient.initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tableId(tableId)
                .tableName(parsedHudiProperties.getTableName())
                .tableType(parsedHudiProperties.getTableType())
                .tableBasePath(table.getRelativeTablePath())
                .databaseName(table.getDatabaseName())
                .lakeName(table.getLakeName())
                .build()))
        .thenReturn(CompletableFuture.completedFuture(initializeTableMetricsCheckpointResponse));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(tableId.toString())
                .commitInstants(filesUploadedFromActiveTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(presignedUrl, presignedUrl))
                    .build()));
    when(onehouseApiClient.generateCommitMetadataUploadUrl(
            GenerateCommitMetadataUploadUrlRequest.builder()
                .tableId(tableId.toString())
                .commitInstants(filesUploadedFromArchivedTimeline)
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(List.of(presignedUrl))
                    .build()));
    when(presignedUrlFileUploader.uploadFileToPresignedUrl(eq(presignedUrl), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED)
                .tableId(tableId)
                .checkpoint(archivedTimelineCheckpoint)
                .filesUploaded(filesUploadedFromArchivedTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));
    when(onehouseApiClient.upsertTableMetricsCheckpoint(
            UpsertTableMetricsCheckpointRequest.builder()
                .commitTimelineType(CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE)
                .tableId(tableId)
                .checkpoint(activeTimelineCheckpoint)
                .filesUploaded(filesUploadedFromActiveTimeline)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(table));
  }

  private File generateFileObj(String fileName, boolean isDirectory) {
    return File.builder()
        .filename(fileName)
        .isDirectory(isDirectory)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }
}
