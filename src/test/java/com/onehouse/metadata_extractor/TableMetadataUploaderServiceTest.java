package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ACTIVE_TIMELINE_CHECKPOINT;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_ARCHIVED_TIMELINE_CHECKPOINT;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.models.request.TableType;
import com.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.metadata_extractor.models.Checkpoint;
import com.onehouse.metadata_extractor.models.ParsedHudiProperties;
import com.onehouse.metadata_extractor.models.Table;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableMetadataUploaderServiceTest {
  @Mock private HoodiePropertiesReader hoodiePropertiesReader;
  @Mock private OnehouseApiClient onehouseApiClient;
  @Mock private TimelineCommitInstantsUploader s3TimelineCommitInstantsUploader;
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

  @BeforeEach
  void setup() {
    mapper.registerModule(new JavaTimeModule());
    tableMetadataUploaderService =
        new TableMetadataUploaderService(
            hoodiePropertiesReader,
            onehouseApiClient,
            s3TimelineCommitInstantsUploader,
            ForkJoinPool.commonPool());
  }

  @Test
  void testUploadMetadataOfANewlyDiscoveredTable() {
    GetTableMetricsCheckpointResponse mockedResponse =
        GetTableMetricsCheckpointResponse.builder().build();
    mockedResponse.setError(404, "");

    InitializeTableMetricsCheckpointResponse initializeTableMetricsCheckpointResponse =
        InitializeTableMetricsCheckpointResponse.builder().build();

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
    when(s3TimelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ARCHIVED_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED))
        .thenReturn(CompletableFuture.completedFuture(true));
    when(s3TimelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(true));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    verify(onehouseApiClient, times(1))
        .initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tableId(TABLE_ID)
                .tableName(PARSED_HUDI_PROPERTIES.getTableName())
                .tableType(PARSED_HUDI_PROPERTIES.getTableType())
                .tableBasePath(TABLE.getRelativeTablePath())
                .databaseName(TABLE.getDatabaseName())
                .lakeName(TABLE.getLakeName())
                .build());
    verify(s3TimelineCommitInstantsUploader, times(1))
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ARCHIVED_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verify(s3TimelineCommitInstantsUploader, times(1))
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  @Test
  @SneakyThrows
  void testUploadMetadataFromPreviousCheckpointArchivedNotProcessed() {
    // few commits from archived timeline have been previously processed
    Checkpoint currentCheckpoint =
        generateCheckpointObj(1, Instant.EPOCH, false, "archived_instant1");
    String currentCheckpointJson = mapper.writeValueAsString(currentCheckpoint);

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder()
                    .checkpoint(currentCheckpointJson)
                    .build()));
    when(s3TimelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, currentCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED))
        .thenReturn(CompletableFuture.completedFuture(true));
    when(s3TimelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(true));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    verify(s3TimelineCommitInstantsUploader, times(1))
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, currentCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verify(s3TimelineCommitInstantsUploader, times(1))
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID,
            TABLE,
            INITIAL_ACTIVE_TIMELINE_CHECKPOINT,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  static Stream<Arguments> provideLastUploadedFileAndExpectedCheckpoint() {
    return Stream.of(
        // case where active-timeline processing has already started
        Arguments.of("active_instant1", false),
        // case where archived-timeline processing has just completed
        Arguments.of(".commits_.archive.48_1-0-1", true));
  }

  @ParameterizedTest
  @MethodSource("provideLastUploadedFileAndExpectedCheckpoint")
  @SneakyThrows
  void testUploadMetadataFromPreviousCheckpointArchivedProcessedActiveTimelineProcessingStarted(
      String lastUploadedFile, boolean useInitialCheckpoint) {
    // archived timeline has already been processed
    Checkpoint currentCheckpoint = generateCheckpointObj(1, Instant.EPOCH, true, lastUploadedFile);
    String currentCheckpointJson = mapper.writeValueAsString(currentCheckpoint);

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder()
                    .checkpoint(currentCheckpointJson)
                    .build()));
    Checkpoint expectedCheckpoint =
        useInitialCheckpoint ? INITIAL_ACTIVE_TIMELINE_CHECKPOINT : currentCheckpoint;
    when(s3TimelineCommitInstantsUploader.uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, expectedCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(true));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // should skip processing archived timeline and directly move to active
    verify(s3TimelineCommitInstantsUploader, times(1))
        .uploadInstantsInTimelineSinceCheckpoint(
            TABLE_ID, TABLE, expectedCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
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
}
