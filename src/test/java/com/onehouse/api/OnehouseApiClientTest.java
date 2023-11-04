package com.onehouse.api;

import static com.onehouse.api.ApiConstants.GENERATE_COMMIT_METADATA_UPLOAD_URL;
import static com.onehouse.api.ApiConstants.GET_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.INITIALIZE_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.UPSERT_TABLE_METRICS_CHECKPOINT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.Map;
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

  private OnehouseApiClient onehouseApiClient;

  @Mock private Call call;
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
    stubOkHttpCall(apiEndpoint);
    CompletableFuture<Map> futureResult =
        onehouseApiClient.asyncPost(apiEndpoint, requestJson, Map.class);
    Map<String, String> result = futureResult.join();
    assertEquals("responseValue", result.get("responseKey"));
  }

  @Test
  void testAsyncGet() {
    String apiEndpoint = "/testEndpoint";
    stubOkHttpCall(apiEndpoint);
    CompletableFuture<Map> futureResult = onehouseApiClient.asyncGet(apiEndpoint, Map.class);
    Map<String, String> result = futureResult.join();
    assertEquals("responseValue", result.get("responseKey"));
  }

  private void stubOkHttpCall(String apiEndpoint) {
    String responseBodyContent = "{\"responseKey\":\"responseValue\"}";
    ResponseBody responseBody =
        ResponseBody.create(responseBodyContent, MediaType.parse("application/json"));
    Response response =
        new Response.Builder()
            .code(200)
            .message("OK")
            .request(new Request.Builder().url("http://example.com" + apiEndpoint).build())
            .protocol(Protocol.HTTP_1_1)
            .body(responseBody)
            .build();

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
            eq(MessageFormat.format(INITIALIZE_TABLE_METRICS_CHECKPOINT, tableId)),
            eq(MAPPER.writeValueAsString(request)),
            eq(InitializeTableMetricsCheckpointResponse.class));
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
            eq(MessageFormat.format(GET_TABLE_METRICS_CHECKPOINT, tableId)),
            eq(GetTableMetricsCheckpointResponse.class));
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
            eq(MessageFormat.format(UPSERT_TABLE_METRICS_CHECKPOINT, tableId)),
            eq(MAPPER.writeValueAsString(request)),
            eq(UpsertTableMetricsCheckpointResponse.class));
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
            eq(MessageFormat.format(GENERATE_COMMIT_METADATA_UPLOAD_URL, tableId)),
            eq(MAPPER.writeValueAsString(request)),
            eq(GenerateCommitMetadataUploadUrlResponse.class));
    GenerateCommitMetadataUploadUrlResponse response =
        onehouseApiClientSpy.generateCommitMetadataUploadUrl(request).get();
    assertNotNull(response);
  }
}