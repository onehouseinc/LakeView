package ai.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.onehouse.api.OnehouseApiClient;
import ai.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import ai.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import ai.onehouse.api.models.response.ApiResponse;
import ai.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import ai.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import ai.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import ai.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metadata_extractor.models.Checkpoint;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.PresignedUrlFileUploader;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestIcebergMetadataUploaderService {

  private static final String TABLE_ID = "table-1";
  private static final String TABLE_URI = "s3://bucket/db/t";
  private static final String LAKE = "lake";
  private static final String DATABASE = "db";

  @Mock private OnehouseApiClient apiClient;
  @Mock private AsyncStorageClient storageClient;
  @Mock private PresignedUrlFileUploader uploader;
  @Mock private LakeViewExtractorMetrics metrics;

  private final StorageUtils storageUtils = new StorageUtils();
  private final String metadataDir = storageUtils.constructFileUri(TABLE_URI, "metadata");
  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  private IcebergMetadataUploaderService service;

  @BeforeEach
  void setUp() {
    service =
        new IcebergMetadataUploaderService(
            storageClient, apiClient, uploader, storageUtils, metrics);
  }

  // ---------------------------------------------------------------------------
  // uploadInstantsInTables / aggregation
  // ---------------------------------------------------------------------------

  @Test
  void emptyTableSetCompletesTrueWithoutApiCalls() {
    assertTrue(service.uploadInstantsInTables(Collections.emptySet()).join());
    verifyNoInteractions(apiClient);
    verifyNoInteractions(storageClient);
  }

  @Test
  void multiTableOneFailureAggregatesToFalse() {
    Table ok = icebergTable(TABLE_ID, null);
    Table bad = icebergTable("table-2", null);
    // table-1: up-to-date no-op (true). table-2: checkpoint fetch fails (false).
    when(apiClient.getTableMetricsCheckpoints(Collections.singletonList(TABLE_ID)))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v1.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(completed(Collections.singletonList(jsonFile("v1.metadata.json"))));
    when(apiClient.getTableMetricsCheckpoints(Collections.singletonList("table-2")))
        .thenReturn(failed(new GetTableMetricsCheckpointResponse(), 500, "boom"));

    assertFalse(service.uploadInstantsInTables(ImmutableSet.of(ok, bad)).join());
  }

  // ---------------------------------------------------------------------------
  // checkpoint fetch / parse
  // ---------------------------------------------------------------------------

  @Test
  void checkpointFetchFailureReturnsFalseAndIncrementsMetric() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(failed(new GetTableMetricsCheckpointResponse(), 503, "down"));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR), anyString());
    verify(apiClient, never()).generateCommitMetadataUploadUrl(any());
  }

  @Test
  void processTableExceptionIncrementsUnknownAndReturnsFalse() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(failedFuture(new RuntimeException("kaboom")));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.UNKNOWN), anyString());
  }

  @Test
  void malformedCheckpointIsTreatedAsMissingAndInitialises() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, "{not-valid-json"));
    when(apiClient.initializeTableMetricsCheckpoint(any())).thenReturn(initOk());
    // INITIAL_CHECKPOINT has lastUploadedFile="", so v1 is new -> full upload.
    stubFullUpload(Collections.singletonList(jsonFile("v1.metadata.json")));

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(apiClient)
        .initializeTableMetricsCheckpoint(any(InitializeTableMetricsCheckpointRequest.class));
  }

  // ---------------------------------------------------------------------------
  // initialise path
  // ---------------------------------------------------------------------------

  @Test
  void noCheckpointInitialisesThenUploads() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, null)); // blank checkpoint -> missing
    when(apiClient.initializeTableMetricsCheckpoint(any())).thenReturn(initOk());
    stubFullUpload(Collections.singletonList(jsonFile("v1.metadata.json")));

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(apiClient)
        .initializeTableMetricsCheckpoint(any(InitializeTableMetricsCheckpointRequest.class));
    verify(metrics).incrementMetadataUploadSuccessCounter();
  }

  @Test
  void initFailureReturnsFalseAndIncrementsMetric() {
    when(apiClient.getTableMetricsCheckpoints(anyList())).thenReturn(okCheckpoint(TABLE_ID, null));
    when(apiClient.initializeTableMetricsCheckpoint(any()))
        .thenReturn(failed(new InitializeTableMetricsCheckpointResponse(), 500, "init failed"));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR), anyString());
    verify(apiClient, never()).generateCommitMetadataUploadUrl(any());
  }

  // ---------------------------------------------------------------------------
  // fallback listing path
  // ---------------------------------------------------------------------------

  @Test
  void fallbackUpToDateIsNoOp() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v3.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(
            completed(
                Arrays.asList(jsonFile("v2.metadata.json"), jsonFile("v3.metadata.json"))));

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(apiClient, never()).generateCommitMetadataUploadUrl(any());
  }

  @Test
  void fallbackAdvancesUploadsAndUpserts() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v2.metadata.json")));
    stubFullUpload(Arrays.asList(jsonFile("v2.metadata.json"), jsonFile("v3.metadata.json")));

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(uploader).uploadFileToPresignedUrl(eq("https://presigned"), anyString(), anyInt());
    verify(metrics).incrementMetadataUploadSuccessCounter();
    verify(apiClient).upsertTableMetricsCheckpoint(any(UpsertTableMetricsCheckpointRequest.class));
  }

  @Test
  void noMetadataJsonReturnsFalseWithNoSuchKey() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v1.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(completed(Collections.singletonList(jsonFile("README.md"))));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.NO_SUCH_KEY), anyString());
  }

  // ---------------------------------------------------------------------------
  // version-hint.text resolution
  // ---------------------------------------------------------------------------

  @Test
  void versionHintResolvesTargetAndUploads() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v1.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(
            completed(
                Arrays.asList(
                    jsonFile("version-hint.text"),
                    jsonFile("v1.metadata.json"),
                    jsonFile("v2.metadata.json"),
                    jsonFile("v7.metadata.json"))));
    when(storageClient.readFileAsBytes(
            storageUtils.constructFileUri(metadataDir, "version-hint.text")))
        .thenReturn(completed("7".getBytes(StandardCharsets.UTF_8)));
    stubGenerateUpload();
    when(apiClient.upsertTableMetricsCheckpoint(any())).thenReturn(upsertOk());

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(apiClient).generateCommitMetadataUploadUrl(argThatCommitInstantIs("v7.metadata.json"));
  }

  @Test
  void versionHintReadFailureFallsBackToNumericSort() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v5.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(
            completed(
                Arrays.asList(
                    jsonFile("version-hint.text"),
                    jsonFile("v5.metadata.json"),
                    jsonFile("v6.metadata.json"))));
    when(storageClient.readFileAsBytes(anyString()))
        .thenReturn(failedFuture(new RuntimeException("read failed")));
    stubGenerateUpload();
    when(apiClient.upsertTableMetricsCheckpoint(any())).thenReturn(upsertOk());

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(apiClient).generateCommitMetadataUploadUrl(argThatCommitInstantIs("v6.metadata.json"));
  }

  // ---------------------------------------------------------------------------
  // metadataLocationHint fast path
  // ---------------------------------------------------------------------------

  @Test
  void hintPathUpToDateSkipsListing() {
    String hint = "s3://bucket/db/t/metadata/v9.metadata.json";
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v9.metadata.json")));

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, hint))).join());
    verify(storageClient, never()).listAllFilesInDir(anyString());
    verify(apiClient, never()).generateCommitMetadataUploadUrl(any());
  }

  @Test
  void hintPathUploadsWhenAdvancedWithoutListing() {
    String hint = "s3a://bucket/db/t/metadata/v9.metadata.json";
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v8.metadata.json")));
    stubGenerateUpload();
    when(apiClient.upsertTableMetricsCheckpoint(any())).thenReturn(upsertOk());

    assertTrue(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, hint))).join());
    verify(storageClient, never()).listAllFilesInDir(anyString());
    verify(uploader).uploadFileToPresignedUrl(eq("https://presigned"), eq(hint), anyInt());
    verify(apiClient).generateCommitMetadataUploadUrl(argThatCommitInstantIs("v9.metadata.json"));
  }

  // ---------------------------------------------------------------------------
  // upload / upsert failure branches
  // ---------------------------------------------------------------------------

  @Test
  void presignedUrlFailureReturnsFalse() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v1.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(completed(Collections.singletonList(jsonFile("v2.metadata.json"))));
    when(apiClient.generateCommitMetadataUploadUrl(any()))
        .thenReturn(failed(new GenerateCommitMetadataUploadUrlResponse(), 500, "no url"));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.PRESIGNED_URL_UPLOAD_FAILURE),
            anyString());
    verify(uploader, never()).uploadFileToPresignedUrl(anyString(), anyString(), anyInt());
  }

  @Test
  void upsertFailureReturnsFalseAfterSuccessfulUpload() {
    when(apiClient.getTableMetricsCheckpoints(anyList()))
        .thenReturn(okCheckpoint(TABLE_ID, checkpointJson("v1.metadata.json")));
    when(storageClient.listAllFilesInDir(metadataDir))
        .thenReturn(completed(Collections.singletonList(jsonFile("v2.metadata.json"))));
    stubGenerateUpload();
    when(apiClient.upsertTableMetricsCheckpoint(any()))
        .thenReturn(failed(new UpsertTableMetricsCheckpointResponse(), 500, "upsert failed"));

    assertFalse(service.uploadInstantsInTables(singleton(icebergTable(TABLE_ID, null))).join());
    verify(metrics).incrementMetadataUploadSuccessCounter(); // upload itself succeeded
    verify(metrics)
        .incrementTableMetadataProcessingFailureCounter(
            eq(MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR), anyString());
  }

  // ===========================================================================
  // Static helper tests (pure, no mocks)
  // ===========================================================================

  @Test
  void picksLatestHiveGlueStyleMetadataJsonByNumericPrefix() {
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                jsonFile("00000-abc.metadata.json"),
                jsonFile("00020-xyz.metadata.json"),
                jsonFile("00005-def.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("00020-xyz.metadata.json", latest.get().getFilename());
  }

  @Test
  void picksLatestHadoopCatalogMetadataJsonByVersionNumber() {
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                jsonFile("v1.metadata.json"),
                jsonFile("v2.metadata.json"),
                jsonFile("v9.metadata.json"),
                jsonFile("v10.metadata.json"),
                jsonFile("v11.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("v11.metadata.json", latest.get().getFilename());
  }

  @Test
  void ignoresDirectoriesAndUnrelatedFiles() {
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                jsonFile("snap-x.avro"),
                File.builder().filename("sub").isDirectory(true).lastModifiedAt(Instant.EPOCH).build(),
                jsonFile("00001-a.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("00001-a.metadata.json", latest.get().getFilename());
  }

  @Test
  void returnsEmptyWhenNoMetadataJson() {
    assertFalse(
        IcebergMetadataUploaderService.pickLatestMetadataJson(
                Collections.singletonList(jsonFile("snap.avro")))
            .isPresent());
  }

  @Test
  void extractVersionNumberHandlesAllNamingShapes() {
    assertEquals(10L, IcebergMetadataUploaderService.extractVersionNumber("v10.metadata.json"));
    assertEquals(
        20L, IcebergMetadataUploaderService.extractVersionNumber("00020-uuid.metadata.json"));
    assertEquals(
        0L, IcebergMetadataUploaderService.extractVersionNumber("00000-uuid.metadata.json"));
    assertEquals(-1L, IcebergMetadataUploaderService.extractVersionNumber("metadata.json"));
    assertEquals(-1L, IcebergMetadataUploaderService.extractVersionNumber("v.metadata.json"));
  }

  @Test
  void resolveFromVersionHintReturnsTargetWhenPresent() {
    Optional<File> resolved =
        IcebergMetadataUploaderService.resolveFromVersionHint(
            "7\n".getBytes(StandardCharsets.UTF_8),
            Arrays.asList(
                jsonFile("v6.metadata.json"),
                jsonFile("v7.metadata.json"),
                jsonFile("v8.metadata.json")));
    assertTrue(resolved.isPresent());
    assertEquals("v7.metadata.json", resolved.get().getFilename());
  }

  @Test
  void resolveFromVersionHintReturnsEmptyWhenTargetMissing() {
    assertFalse(
        IcebergMetadataUploaderService.resolveFromVersionHint(
                "42".getBytes(StandardCharsets.UTF_8),
                Arrays.asList(jsonFile("v6.metadata.json"), jsonFile("v7.metadata.json")))
            .isPresent());
  }

  @Test
  void resolveFromVersionHintReturnsEmptyOnNonIntegerContent() {
    assertFalse(
        IcebergMetadataUploaderService.resolveFromVersionHint(
                "not-an-int".getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(jsonFile("v1.metadata.json")))
            .isPresent());
  }

  @Test
  void lastPathSegmentExtractsFilenameFromUri() {
    assertEquals(
        "00042-uuid.metadata.json",
        IcebergMetadataUploaderService.lastPathSegment(
            "s3://bucket/db/t/metadata/00042-uuid.metadata.json"));
    assertEquals(
        "v3.metadata.json",
        IcebergMetadataUploaderService.lastPathSegment(
            "s3a://bucket/db/t/metadata/v3.metadata.json"));
  }

  @Test
  void lastPathSegmentReturnsInputWhenNoSlash() {
    assertEquals("filename.json", IcebergMetadataUploaderService.lastPathSegment("filename.json"));
  }

  // ===========================================================================
  // helpers
  // ===========================================================================

  private static Set<Table> singleton(Table t) {
    return Collections.singleton(t);
  }

  private static Table icebergTable(String tableId, String metadataLocationHint) {
    return Table.builder()
        .tableId(tableId)
        .absoluteTableUri(TABLE_URI)
        .lakeName(LAKE)
        .databaseName(DATABASE)
        .tableFormat(TableFormat.ICEBERG)
        .metadataLocationHint(metadataLocationHint)
        .build();
  }

  private String checkpointJson(String lastUploadedFile) {
    Checkpoint checkpoint =
        Checkpoint.builder()
            .batchId(1)
            .checkpointTimestamp(Instant.EPOCH)
            .lastUploadedFile(lastUploadedFile)
            .firstIncompleteCommitFile("")
            .archivedCommitsProcessed(true)
            .build();
    try {
      return mapper.writeValueAsString(checkpoint);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static CompletableFuture<GetTableMetricsCheckpointResponse> okCheckpoint(
      String tableId, String checkpointJson) {
    return completed(
        GetTableMetricsCheckpointResponse.builder()
            .checkpoints(
                Collections.singletonList(
                    GetTableMetricsCheckpointResponse.TableMetadataCheckpoint.builder()
                        .tableId(tableId)
                        .checkpoint(checkpointJson)
                        .build()))
            .build());
  }

  private static CompletableFuture<InitializeTableMetricsCheckpointResponse> initOk() {
    return completed(InitializeTableMetricsCheckpointResponse.builder().build());
  }

  private static CompletableFuture<UpsertTableMetricsCheckpointResponse> upsertOk() {
    return completed(UpsertTableMetricsCheckpointResponse.builder().build());
  }

  /** Stubs generate-url (one presigned URL) + a successful presigned PUT. */
  private void stubGenerateUpload() {
    when(apiClient.generateCommitMetadataUploadUrl(any()))
        .thenReturn(
            completed(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(Collections.singletonList("https://presigned"))
                    .build()));
    when(uploader.uploadFileToPresignedUrl(anyString(), anyString(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  /** Stubs the full happy-path: list metadata/, generate url, upload, upsert. */
  private void stubFullUpload(List<File> metadataListing) {
    when(storageClient.listAllFilesInDir(metadataDir)).thenReturn(completed(metadataListing));
    stubGenerateUpload();
    when(apiClient.upsertTableMetricsCheckpoint(any())).thenReturn(upsertOk());
  }

  private static GenerateCommitMetadataUploadUrlRequest argThatCommitInstantIs(String filename) {
    return ArgumentMatchers.argThat(
        req -> req != null && req.getCommitInstants().contains(filename));
  }

  private static <T> CompletableFuture<T> completed(T value) {
    return CompletableFuture.completedFuture(value);
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  private static <T extends ApiResponse> CompletableFuture<T> failed(
      T response, int statusCode, String cause) {
    response.setError(statusCode, cause);
    return CompletableFuture.completedFuture(response);
  }

  private static File jsonFile(String name) {
    return File.builder().filename(name).isDirectory(false).lastModifiedAt(Instant.EPOCH).build();
  }
}
