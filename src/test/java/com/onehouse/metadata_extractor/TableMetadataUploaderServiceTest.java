package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_PROPERTIES_FILE;
import static com.onehouse.constants.MetadataExtractorConstants.INITIAL_CHECKPOINT;
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
  @Mock private TimelineCommitInstantsUploader timelineCommitInstantsUploader;
  private TableMetadataUploaderService tableMetadataUploaderService;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final String S3_TABLE_URI = "s3://bucket/table/";
  private static final Table TABLE =
      Table.builder()
          .absoluteTableUri(S3_TABLE_URI)
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
  private final Checkpoint FINAL_ARCHIVED_TIMELINE_CHECKPOINT =
      generateCheckpointObj(3, Instant.now(), true, ".commits_.archive.48_1-0-1");
  private final Checkpoint FINAL_ARCHIVED_TIMELINE_CHECKPOINT_WITH_RESET_FIELDS =
      generateCheckpointObj(3, Instant.EPOCH, true, "");
  private final Checkpoint FINAL_ACTIVE_TIMELINE_CHECKPOINT =
      generateCheckpointObj(6, Instant.now(), true, "active_instant");

  @BeforeEach
  void setup() {
    mapper.registerModule(new JavaTimeModule());
    tableMetadataUploaderService =
        new TableMetadataUploaderService(
            hoodiePropertiesReader,
            onehouseApiClient,
            timelineCommitInstantsUploader,
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
                .databaseName(TABLE.getDatabaseName())
                .lakeName(TABLE.getLakeName())
                .build()))
        .thenReturn(CompletableFuture.completedFuture(initializeTableMetricsCheckpointResponse));
    when(timelineCommitInstantsUploader.batchUploadWithCheckpoint(
            TABLE_ID, TABLE, INITIAL_CHECKPOINT, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED))
        .thenReturn(CompletableFuture.completedFuture(FINAL_ARCHIVED_TIMELINE_CHECKPOINT));
    when(timelineCommitInstantsUploader.paginatedBatchUploadWithCheckpoint(
            TABLE_ID,
            TABLE,
            FINAL_ARCHIVED_TIMELINE_CHECKPOINT_WITH_RESET_FIELDS,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(FINAL_ACTIVE_TIMELINE_CHECKPOINT));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    verify(onehouseApiClient, times(1))
        .initializeTableMetricsCheckpoint(
            InitializeTableMetricsCheckpointRequest.builder()
                .tableId(TABLE_ID)
                .tableName(PARSED_HUDI_PROPERTIES.getTableName())
                .tableType(PARSED_HUDI_PROPERTIES.getTableType())
                .databaseName(TABLE.getDatabaseName())
                .lakeName(TABLE.getLakeName())
                .build());
    verify(timelineCommitInstantsUploader, times(1))
        .batchUploadWithCheckpoint(
            TABLE_ID, TABLE, INITIAL_CHECKPOINT, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verify(timelineCommitInstantsUploader, times(1))
        .paginatedBatchUploadWithCheckpoint(
            TABLE_ID,
            TABLE,
            FINAL_ARCHIVED_TIMELINE_CHECKPOINT_WITH_RESET_FIELDS,
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
    when(timelineCommitInstantsUploader.batchUploadWithCheckpoint(
            TABLE_ID, TABLE, currentCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED))
        .thenReturn(CompletableFuture.completedFuture(FINAL_ARCHIVED_TIMELINE_CHECKPOINT));
    when(timelineCommitInstantsUploader.paginatedBatchUploadWithCheckpoint(
            TABLE_ID,
            TABLE,
            FINAL_ARCHIVED_TIMELINE_CHECKPOINT_WITH_RESET_FIELDS,
            CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(FINAL_ACTIVE_TIMELINE_CHECKPOINT));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    verify(timelineCommitInstantsUploader, times(1))
        .batchUploadWithCheckpoint(
            TABLE_ID, TABLE, currentCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ARCHIVED);
    verify(timelineCommitInstantsUploader, times(1))
        .paginatedBatchUploadWithCheckpoint(
            TABLE_ID,
            TABLE,
            FINAL_ARCHIVED_TIMELINE_CHECKPOINT_WITH_RESET_FIELDS,
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
      String lastUploadedFile, boolean shouldResetCheckpoint) {
    // archived timeline has already been processed
    Checkpoint currentCheckpoint = generateCheckpointObj(1, Instant.now(), true, lastUploadedFile);
    Checkpoint currentCheckpointWithResetFields =
        currentCheckpoint.toBuilder()
            .checkpointTimestamp(Instant.EPOCH)
            .lastUploadedFile("")
            .build();
    String currentCheckpointJson = mapper.writeValueAsString(currentCheckpoint);

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder()
                    .checkpoint(currentCheckpointJson)
                    .build()));
    Checkpoint expectedCheckpoint =
        shouldResetCheckpoint ? currentCheckpointWithResetFields : currentCheckpoint;
    when(timelineCommitInstantsUploader.paginatedBatchUploadWithCheckpoint(
            TABLE_ID, TABLE, expectedCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE))
        .thenReturn(CompletableFuture.completedFuture(FINAL_ACTIVE_TIMELINE_CHECKPOINT));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    // should skip processing archived timeline and directly move to active
    verify(timelineCommitInstantsUploader, times(1))
        .paginatedBatchUploadWithCheckpoint(
            TABLE_ID, TABLE, expectedCheckpoint, CommitTimelineType.COMMIT_TIMELINE_TYPE_ACTIVE);
  }

  @Test
  void testUploadMetadataWhenGetCheckpointFails() {
    GetTableMetricsCheckpointResponse mockedResponse =
        GetTableMetricsCheckpointResponse.builder().build();
    mockedResponse.setError(500, ""); // any non 404 error

    when(onehouseApiClient.getTableMetricsCheckpoint(TABLE_ID.toString()))
        .thenReturn(CompletableFuture.completedFuture(mockedResponse));

    tableMetadataUploaderService.uploadInstantsInTables(Set.of(TABLE)).join();

    verify(onehouseApiClient, times(1))
        .getTableMetricsCheckpoint(
            TABLE_ID.toString()); // placeholder to ensure that onehouseApiClient is interacted with
    // just once
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
