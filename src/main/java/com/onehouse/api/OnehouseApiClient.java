package com.onehouse.api;

import static com.onehouse.api.ApiConstants.GENERATE_COMMIT_METADATA_UPLOAD_URL;
import static com.onehouse.api.ApiConstants.GET_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.INITIALIZE_TABLE_METRICS_CHECKPOINT;
import static com.onehouse.api.ApiConstants.ONEHOUSE_API_ENDPOINT;
import static com.onehouse.api.ApiConstants.ONEHOUSE_API_KEY;
import static com.onehouse.api.ApiConstants.ONEHOUSE_API_SECRET_KEY;
import static com.onehouse.api.ApiConstants.ONEHOUSE_REGION_KEY;
import static com.onehouse.api.ApiConstants.ONEHOUSE_USER_UUID_KEY;
import static com.onehouse.api.ApiConstants.PROJECT_UID_KEY;
import static com.onehouse.api.ApiConstants.UPSERT_TABLE_METRICS_CHECKPOINT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.onehouse.api.request.GenerateCommitMetadataUploadUrlRequest;
import com.onehouse.api.request.InitializeTableMetricsCheckpointRequest;
import com.onehouse.api.request.UpsertTableMetricsCheckpointRequest;
import com.onehouse.api.response.ApiResponse;
import com.onehouse.api.response.GenerateCommitMetadataUploadUrlResponse;
import com.onehouse.api.response.GetTableMetricsCheckpointResponse;
import com.onehouse.api.response.InitializeTableMetricsCheckpointResponse;
import com.onehouse.api.response.UpsertTableMetricsCheckpointResponse;
import com.onehouse.config.Config;
import com.onehouse.config.common.OnehouseClientConfig;
import com.onehouse.config.configv1.ConfigV1;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class OnehouseApiClient {
  private final OkHttpClient okHttpClient;
  private final Headers headers;
  private final ObjectMapper mapper;

  @Inject
  public OnehouseApiClient(@Nonnull OkHttpClient okHttpClient, @Nonnull Config config) {
    this.okHttpClient = okHttpClient;
    this.headers = getHeaders(((ConfigV1) config).getOnehouseClientConfig());
    this.mapper = new ObjectMapper();
  }

  @SneakyThrows
  public CompletableFuture<InitializeTableMetricsCheckpointResponse>
      initializeTableMetricsCheckpoint(InitializeTableMetricsCheckpointRequest request) {
    return asyncPost(
        MessageFormat.format(INITIALIZE_TABLE_METRICS_CHECKPOINT, request.getTableId()),
        mapper.writeValueAsString(request),
        InitializeTableMetricsCheckpointResponse.class);
  }

  @SneakyThrows
  public CompletableFuture<GetTableMetricsCheckpointResponse> getTableMetricsCheckpoint(
      String tableId) {
    return asyncGet(
        MessageFormat.format(GET_TABLE_METRICS_CHECKPOINT, tableId),
        GetTableMetricsCheckpointResponse.class);
  }

  @SneakyThrows
  public CompletableFuture<UpsertTableMetricsCheckpointResponse> upsertTableMetricsCheckpoint(
      UpsertTableMetricsCheckpointRequest request) {
    return asyncPost(
        MessageFormat.format(UPSERT_TABLE_METRICS_CHECKPOINT, request.getTableId()),
        mapper.writeValueAsString(request),
        UpsertTableMetricsCheckpointResponse.class);
  }

  @SneakyThrows
  public CompletableFuture<GenerateCommitMetadataUploadUrlResponse> generateCommitMetadataUploadUrl(
      GenerateCommitMetadataUploadUrlRequest request) {
    return asyncPost(
        MessageFormat.format(GENERATE_COMMIT_METADATA_UPLOAD_URL, request.getTableId()),
        mapper.writeValueAsString(request),
        GenerateCommitMetadataUploadUrlResponse.class);
  }

  private Headers getHeaders(OnehouseClientConfig onehouseClientConfig) {
    Headers.Builder headersBuilder = new Headers.Builder();
    headersBuilder.add(PROJECT_UID_KEY, onehouseClientConfig.getProjectId());
    headersBuilder.add(ONEHOUSE_API_KEY, onehouseClientConfig.getApiKey());
    headersBuilder.add(ONEHOUSE_API_SECRET_KEY, onehouseClientConfig.getApiSecret());
    headersBuilder.add(ONEHOUSE_REGION_KEY, onehouseClientConfig.getRegion());
    headersBuilder.add(ONEHOUSE_USER_UUID_KEY, onehouseClientConfig.getUserUuid());
    return headersBuilder.build();
  }

  @VisibleForTesting
  <T> CompletableFuture<T> asyncGet(String apiEndpoint, Class<T> typeReference) {
    Request request =
        new Request.Builder().url(ONEHOUSE_API_ENDPOINT + apiEndpoint).headers(headers).build();

    OkHttpResponseFuture callback = new OkHttpResponseFuture();
    okHttpClient.newCall(request).enqueue(callback);
    return callback.future.thenApply(response -> handleResponse(response, typeReference));
  }

  @VisibleForTesting
  <T> CompletableFuture<T> asyncPost(String apiEndpoint, String json, Class<T> typeReference) {
    RequestBody body = RequestBody.create(json, MediaType.parse("application/json; charset=utf-8"));

    Request request =
        new Request.Builder()
            .url(ONEHOUSE_API_ENDPOINT + apiEndpoint)
            .post(body)
            .headers(headers)
            .build();

    OkHttpResponseFuture callback = new OkHttpResponseFuture();
    okHttpClient.newCall(request).enqueue(callback);
    return callback.future.thenApply(response -> handleResponse(response, typeReference));
  }

  private <T> T handleResponse(Response response, Class<T> typeReference) {
    if (response.isSuccessful()) {
      try {
        if (response.body() != null) {
          return mapper.readValue(response.body().string(), typeReference);
        }
        return null;
      } catch (IOException jsonProcessingException) {
        throw new UncheckedIOException("Failed to deserialize", jsonProcessingException);
      }
    } else {
      try {
        T errorResponse = typeReference.getDeclaredConstructor().newInstance();
        if (errorResponse instanceof ApiResponse) {
          ((ApiResponse) errorResponse).setError(response.code(), response.message());
        }
        return errorResponse;
      } catch (InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        throw new RuntimeException("Failed to instantiate error response object", e);
      }
    }
  }
}