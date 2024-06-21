package com.onehouse.api;

import static com.onehouse.constants.ApiConstants.GENERATE_COMMIT_METADATA_UPLOAD_URL;
import static com.onehouse.constants.ApiConstants.GET_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.constants.ApiConstants.INITIALIZE_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.constants.ApiConstants.LINK_UID_KEY;
import static com.onehouse.constants.ApiConstants.ONEHOUSE_API_ENDPOINT;
import static com.onehouse.constants.ApiConstants.ONEHOUSE_API_KEY;
import static com.onehouse.constants.ApiConstants.ONEHOUSE_API_SECRET_KEY;
import static com.onehouse.constants.ApiConstants.ONEHOUSE_REGION_KEY;
import static com.onehouse.constants.ApiConstants.ONEHOUSE_USER_UUID_KEY;
import static com.onehouse.constants.ApiConstants.PROJECT_UID_KEY;
import static com.onehouse.constants.ApiConstants.UPSERT_TABLE_METRICS_CHECKPOINT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onehouse.api.models.request.CommitTimelineType;
import com.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.models.request.TableType;
import com.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.config.models.common.OnehouseClientConfig;
import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.constants.MetricsConstants;
import com.onehouse.metrics.LakeViewExtractorMetrics;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OnehouseApiClientTest {
  @Mock private AsyncHttpClientWithRetry client;
  @Mock private ConfigV1 config;
  @Mock private OnehouseClientConfig onehouseClientConfig;
  @Mock private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  private OnehouseApiClient onehouseApiClient;

  private static final int FAILURE_STATUS_CODE_USER = 400;
  private static final int FAILURE_STATUS_CODE_SYSTEM = 500;
  private static final String SAMPLE_HOST = "http://example.com";
  private static final String FAILURE_ERROR = "call failed";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final String PROJECT_ID = "projectId";
  public static final String REQUEST_ID = "requestId";
  public static final String REGION = "region";
  public static final String API_KEY = "apiKey";
  public static final String API_SECRET = "apiSecret";
  public static final String USER_ID = "userId";

  @BeforeEach
  void setup() {
    when(config.getOnehouseClientConfig()).thenReturn(onehouseClientConfig);
    when(onehouseClientConfig.getProjectId()).thenReturn(PROJECT_ID);
    when(onehouseClientConfig.getApiKey()).thenReturn(API_KEY);
    when(onehouseClientConfig.getApiSecret()).thenReturn(API_SECRET);
    when(onehouseClientConfig.getUserId()).thenReturn(USER_ID);
    onehouseApiClient = new OnehouseApiClient(client, config, hudiMetadataExtractorMetrics);
  }

  @Test
  void testLinkIdRegionHeaders() {
    Headers headers = onehouseApiClient.getHeaders(onehouseClientConfig);
    assertEquals(
        Headers.of(
            PROJECT_UID_KEY,
            PROJECT_ID,
            ONEHOUSE_API_KEY,
            API_KEY,
            ONEHOUSE_API_SECRET_KEY,
            API_SECRET,
            ONEHOUSE_USER_UUID_KEY,
            USER_ID),
        headers);
    when(onehouseClientConfig.getRequestId()).thenReturn(REQUEST_ID);
    when(onehouseClientConfig.getRegion()).thenReturn(REGION);
    onehouseApiClient = new OnehouseApiClient(client, config, hudiMetadataExtractorMetrics);
    headers = onehouseApiClient.getHeaders(onehouseClientConfig);
    assertEquals(
        Headers.of(
            PROJECT_UID_KEY,
            PROJECT_ID,
            ONEHOUSE_API_KEY,
            API_KEY,
            ONEHOUSE_API_SECRET_KEY,
            API_SECRET,
            ONEHOUSE_USER_UUID_KEY,
            USER_ID,
            LINK_UID_KEY,
            REQUEST_ID,
            ONEHOUSE_REGION_KEY,
            REGION),
        headers);
  }

  @Test
  void testAsyncPost() {
    String apiEndpoint = "/testEndpoint";
    String requestJson = "{\"key\":\"value\"}";
    stubOkHttpCall(apiEndpoint, false);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncPost(
            apiEndpoint, requestJson, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertEquals("checkpoint", result.getCheckpoints().get(0).getCheckpoint());
  }

  @ParameterizedTest
  @ValueSource(ints = {FAILURE_STATUS_CODE_SYSTEM, FAILURE_STATUS_CODE_USER})
  void testAsyncPostFailure(int failureStatusCode) {
    String apiEndpoint = "/testEndpoint";
    String requestJson = "{\"key\":\"value\"}";
    stubOkHttpCall(apiEndpoint, true, failureStatusCode);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncPost(
            apiEndpoint, requestJson, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertTrue(result.isFailure());
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            failureStatusCode == FAILURE_STATUS_CODE_SYSTEM
                ? MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR
                : MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_USER_ERROR);
    assertEquals(failureStatusCode, result.getStatusCode());
  }

  @Test
  void testAsyncGet() {
    String apiEndpoint = "/testEndpoint";
    stubOkHttpCall(apiEndpoint, false);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncGet(
            SAMPLE_HOST + apiEndpoint, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertEquals("checkpoint", result.getCheckpoints().get(0).getCheckpoint());
  }

  @ParameterizedTest
  @ValueSource(ints = {FAILURE_STATUS_CODE_SYSTEM, FAILURE_STATUS_CODE_USER})
  void testAsyncGetFailure(int failureStatusCode) {
    String apiEndpoint = "/testEndpoint";
    stubOkHttpCall(apiEndpoint, true, failureStatusCode);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncGet(
            SAMPLE_HOST + apiEndpoint, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertTrue(result.isFailure());
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            failureStatusCode == FAILURE_STATUS_CODE_SYSTEM
                ? MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR
                : MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_USER_ERROR);
    assertEquals(failureStatusCode, result.getStatusCode());
  }

  static Stream<Arguments> provideInitializeTableMetricsCheckpointValue() {
    return Stream.of(
        Arguments.of(TableType.COPY_ON_WRITE, "lake", "database"),
        Arguments.of(TableType.MERGE_ON_READ, "lake", "database"),
        Arguments.of(TableType.COPY_ON_WRITE, null, null),
        Arguments.of(TableType.MERGE_ON_READ, null, null));
  }

  @ParameterizedTest
  @MethodSource("provideInitializeTableMetricsCheckpointValue")
  @SneakyThrows
  void verifyInitializeTableMetricsCheckpointApi(TableType tableType) {
    UUID tableId = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);
    InitializeTableMetricsCheckpointRequest request =
        InitializeTableMetricsCheckpointRequest.builder()
            .tables(
                Collections.singletonList(
                    InitializeTableMetricsCheckpointRequest
                        .InitializeSingleTableMetricsCheckpointRequest.builder()
                        .tableId(tableId.toString())
                        .tableType(tableType)
                        .tableName("table")
                        .databaseName("database")
                        .lakeName("lake")
                        .tableBasePath("tableBasePath")
                        .build()))
            .build();
    doReturn(
            CompletableFuture.completedFuture(
                InitializeTableMetricsCheckpointResponse.builder().build()))
        .when(onehouseApiClientSpy)
        .asyncPost(
            (MessageFormat.format(INITIALIZE_TABLE_METRICS_CHECKPOINT, tableId)),
            (MAPPER.writeValueAsString(request)),
            (InitializeTableMetricsCheckpointResponse.class));
    InitializeTableMetricsCheckpointResponse response =
        onehouseApiClientSpy.initializeTableMetricsCheckpoint(request).get();
    assertNotNull(response);
  }

  @Test
  @SneakyThrows
  void verifyGetTableMetricsCheckpoint() {
    UUID tableId1 = UUID.randomUUID();
    UUID tableId2 = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);

    doReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder()
                    .checkpoints(
                        Arrays.asList(
                            buildTableMetadataCheckpoint(tableId1.toString(), "checkpoint1"),
                            buildTableMetadataCheckpoint(tableId2.toString(), "checkpoint2")))
                    .build()))
        .when(onehouseApiClientSpy)
        .asyncGet(
            String.format(
                "%s%s?tableIds=%s&tableIds=%s",
                ONEHOUSE_API_ENDPOINT,
                GET_TABLE_METRICS_CHECKPOINT,
                tableId1,
                tableId2), // get request url
            GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse response =
        onehouseApiClientSpy
            .getTableMetricsCheckpoints(Arrays.asList(tableId1.toString(), tableId2.toString()))
            .get();
    assertNotNull(response);
  }

  @ParameterizedTest
  @EnumSource(CommitTimelineType.class)
  @SneakyThrows
  void verifyUpsertTableMetricsCheckpoint(CommitTimelineType commitTimelineType) {
    UUID tableId = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);
    UpsertTableMetricsCheckpointRequest request =
        UpsertTableMetricsCheckpointRequest.builder()
            .tableId(tableId.toString())
            .filesUploaded(Collections.emptyList())
            .uploadedFiles(Collections.emptyList())
            .commitTimelineType(commitTimelineType)
            .checkpoint("")
            .build();
    doReturn(
            CompletableFuture.completedFuture(
                UpsertTableMetricsCheckpointResponse.builder().build()))
        .when(onehouseApiClientSpy)
        .asyncPost(
            (MessageFormat.format(UPSERT_TABLE_METRICS_CHECKPOINT, tableId)),
            (MAPPER.writeValueAsString(request)),
            (UpsertTableMetricsCheckpointResponse.class));
    UpsertTableMetricsCheckpointResponse response =
        onehouseApiClientSpy.upsertTableMetricsCheckpoint(request).get();
    assertNotNull(response);
  }

  @ParameterizedTest
  @EnumSource(CommitTimelineType.class)
  @SneakyThrows
  void verifyGenerateCommitMetadataUploadUrl(CommitTimelineType commitTimelineType) {
    UUID tableId = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);
    GenerateCommitMetadataUploadUrlRequest request =
        GenerateCommitMetadataUploadUrlRequest.builder()
            .tableId(tableId.toString())
            .commitTimelineType(commitTimelineType)
            .commitInstants(Collections.emptyList())
            .build();
    doReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder()
                    .uploadUrls(Collections.emptyList())
                    .build()))
        .when(onehouseApiClientSpy)
        .asyncPost(
            (MessageFormat.format(GENERATE_COMMIT_METADATA_UPLOAD_URL, tableId)),
            (MAPPER.writeValueAsString(request)),
            (GenerateCommitMetadataUploadUrlResponse.class));
    GenerateCommitMetadataUploadUrlResponse response =
        onehouseApiClientSpy.generateCommitMetadataUploadUrl(request).get();
    assertNotNull(response);
  }

  private void stubOkHttpCall(String apiEndpoint, boolean isFailure) {
    stubOkHttpCall(apiEndpoint, isFailure, FAILURE_STATUS_CODE_SYSTEM);
  }

  private void stubOkHttpCall(String apiEndpoint, boolean isFailure, int failureStatusCode) {
    String responseBodyContent =
        "{\"checkpoints\":[{\"checkpoint\":\"checkpoint\",\"tableId\":\"tableId\"}]}";
    ResponseBody responseBody =
        ResponseBody.create(responseBodyContent, MediaType.parse("application/json"));
    Response response;
    if (isFailure) {
      response =
          new Response.Builder()
              .code(failureStatusCode)
              .message(FAILURE_ERROR)
              .request(new Request.Builder().url(SAMPLE_HOST + apiEndpoint).build())
              .protocol(Protocol.HTTP_1_1)
              .body(responseBody)
              .build();
    } else {
      response =
          new Response.Builder()
              .code(200)
              .message("OK")
              .request(new Request.Builder().url(SAMPLE_HOST + apiEndpoint).build())
              .protocol(Protocol.HTTP_1_1)
              .body(responseBody)
              .build();
    }

    when(client.makeRequestWithRetry(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response));
  }

  private GetTableMetricsCheckpointResponse.TableMetadataCheckpoint buildTableMetadataCheckpoint(
      String tableId, String checkpoint) {
    return GetTableMetricsCheckpointResponse.TableMetadataCheckpoint.builder()
        .tableId(tableId)
        .checkpoint(checkpoint)
        .build();
  }
}
