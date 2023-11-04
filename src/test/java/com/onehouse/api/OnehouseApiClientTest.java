package com.onehouse.api;

import static com.onehouse.api.ApiConstants.GENERATE_COMMIT_METADATA_UPLOAD_URL;
import static com.onehouse.api.ApiConstants.GET_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.INITIALIZE_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.UPSERT_TABLE_METRICS_CHECKPOINT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onehouse.api.request.CommitTimelineType;
import com.onehouse.api.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.request.TableType;
import com.onehouse.api.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.api.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.config.common.OnehouseClientConfig;
import com.onehouse.config.configv1.ConfigV1;
import java.text.MessageFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OnehouseApiClientTest {
  @Mock private OkHttpClient okHttpClient;
  @Mock private ConfigV1 config;
  @Mock private OnehouseClientConfig onehouseClientConfig;
  @Mock private Call call;
  private OnehouseApiClient onehouseApiClient;

  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void setup() {
    when(config.getOnehouseClientConfig()).thenReturn(onehouseClientConfig);
    when(onehouseClientConfig.getProjectId()).thenReturn("projectId");
    when(onehouseClientConfig.getApiKey()).thenReturn("apiKey");
    when(onehouseClientConfig.getApiSecret()).thenReturn("apiSecret");
    when(onehouseClientConfig.getRegion()).thenReturn("region");
    when(onehouseClientConfig.getUserUuid()).thenReturn("userUuid");
    onehouseApiClient = new OnehouseApiClient(okHttpClient, config);
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
    assertEquals("checkpoint", result.getCheckpoint());
  }

  @Test
  void testAsyncPostFailure() {
    String apiEndpoint = "/testEndpoint";
    String requestJson = "{\"key\":\"value\"}";
    stubOkHttpCall(apiEndpoint, true);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncPost(
            apiEndpoint, requestJson, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertTrue(result.isFailure());
    assertEquals(FAILURE_STATUS_CODE, result.getStatusCode());
  }

  @Test
  void testAsyncGet() {
    String apiEndpoint = "/testEndpoint";
    stubOkHttpCall(apiEndpoint, false);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncGet(apiEndpoint, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertEquals("checkpoint", result.getCheckpoint());
  }

  @Test
  void testAsyncGetFailure() {
    String apiEndpoint = "/testEndpoint";
    stubOkHttpCall(apiEndpoint, true);
    CompletableFuture<GetTableMetricsCheckpointResponse> futureResult =
        onehouseApiClient.asyncGet(apiEndpoint, GetTableMetricsCheckpointResponse.class);
    GetTableMetricsCheckpointResponse result = futureResult.join();
    assertTrue(result.isFailure());
    assertEquals(FAILURE_STATUS_CODE, result.getStatusCode());
  }

  @ParameterizedTest
  @EnumSource(TableType.class)
  @SneakyThrows
  void verifyInitializeTableMetricsCheckpointApi(TableType tableType) {
    UUID tableId = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);
    InitializeTableMetricsCheckpointRequest request =
        InitializeTableMetricsCheckpointRequest.builder()
            .tableId(tableId)
            .tableType(tableType)
            .tableName("table")
            .databaseName("database")
            .lakeName("lake")
            .tableBasePath("valid/path")
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
    UUID tableId = UUID.randomUUID();
    OnehouseApiClient onehouseApiClientSpy = spy(onehouseApiClient);

    doReturn(
            CompletableFuture.completedFuture(
                GetTableMetricsCheckpointResponse.builder().checkpoint("").build()))
        .when(onehouseApiClientSpy)
        .asyncGet(
            (MessageFormat.format(GET_TABLE_METRICS_CHECKPOINT, tableId)),
            (GetTableMetricsCheckpointResponse.class));
    GetTableMetricsCheckpointResponse response =
        onehouseApiClientSpy.getTableMetricsCheckpoint(String.valueOf(tableId)).get();
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
            .tableId(tableId)
            .filesUploaded(List.of())
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
            .tableId(tableId)
            .commitTimelineType(commitTimelineType)
            .commitInstants(List.of())
            .build();
    doReturn(
            CompletableFuture.completedFuture(
                GenerateCommitMetadataUploadUrlResponse.builder().uploadUrls(List.of()).build()))
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
    String responseBodyContent = "{\"checkpoint\":\"checkpoint\"}";
    ResponseBody responseBody =
        ResponseBody.create(responseBodyContent, MediaType.parse("application/json"));
    Response response;
    if (isFailure) {
      response =
          new Response.Builder()
              .code(FAILURE_STATUS_CODE)
              .message(FAILURE_ERROR)
              .request(new Request.Builder().url("http://example.com" + apiEndpoint).build())
              .protocol(Protocol.HTTP_1_1)
              .build();
    } else {
      response =
          new Response.Builder()
              .code(200)
              .message("OK")
              .request(new Request.Builder().url("http://example.com" + apiEndpoint).build())
              .protocol(Protocol.HTTP_1_1)
              .body(responseBody)
              .build();
    }

    when(okHttpClient.newCall(any(Request.class))).thenReturn(call);
    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(0);
              callback.onResponse(call, response);
              return null;
            })
        .when(call)
        .enqueue(any(Callback.class));
  }
}
