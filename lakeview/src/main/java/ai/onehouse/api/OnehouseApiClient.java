package ai.onehouse.api;

import static ai.onehouse.constants.ApiConstants.ACCEPTABLE_HTTP_FAILURE_STATUS_CODES;
import static ai.onehouse.constants.ApiConstants.GENERATE_COMMIT_METADATA_UPLOAD_URL;
import static ai.onehouse.constants.ApiConstants.GET_TABLE_METRICS_CHECKPOINT;
import static ai.onehouse.constants.ApiConstants.INITIALIZE_TABLE_METRICS_CHECKPOINT;
import static ai.onehouse.constants.ApiConstants.LINK_UID_KEY;
import static ai.onehouse.constants.ApiConstants.MAINTENANCE_MODE_KEY;
import static ai.onehouse.constants.ApiConstants.ONEHOUSE_API_ENDPOINT;
import static ai.onehouse.constants.ApiConstants.ONEHOUSE_API_KEY;
import static ai.onehouse.constants.ApiConstants.ONEHOUSE_API_SECRET_KEY;
import static ai.onehouse.constants.ApiConstants.ONEHOUSE_REGION_KEY;
import static ai.onehouse.constants.ApiConstants.ONEHOUSE_USER_UUID_KEY;
import static ai.onehouse.constants.ApiConstants.PROJECT_UID_KEY;
import static ai.onehouse.constants.ApiConstants.UNAUTHORIZED_ERROR_MESSAGE;
import static ai.onehouse.constants.ApiConstants.UPSERT_TABLE_METRICS_CHECKPOINT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.api.models.request.GenerateCommitMetadataUploadUrlRequest;
import ai.onehouse.api.models.request.InitializeTableMetricsCheckpointRequest;
import ai.onehouse.api.models.request.UpsertTableMetricsCheckpointRequest;
import ai.onehouse.api.models.response.ApiResponse;
import ai.onehouse.api.models.response.GenerateCommitMetadataUploadUrlResponse;
import ai.onehouse.api.models.response.GetTableMetricsCheckpointResponse;
import ai.onehouse.api.models.response.InitializeTableMetricsCheckpointResponse;
import ai.onehouse.api.models.response.UpsertTableMetricsCheckpointResponse;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.OnehouseClientConfig;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;

public class OnehouseApiClient {
  private final AsyncHttpClientWithRetry asyncClient;
  private final Headers headers;
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  private final ObjectMapper mapper;

  @Inject
  public OnehouseApiClient(
      @Nonnull AsyncHttpClientWithRetry asyncClient,
      @Nonnull Config config,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncClient = asyncClient;
    this.headers = getHeaders(config.getOnehouseClientConfig());
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
    this.mapper = new ObjectMapper();
  }

  @SneakyThrows
  public CompletableFuture<InitializeTableMetricsCheckpointResponse>
      initializeTableMetricsCheckpoint(InitializeTableMetricsCheckpointRequest request) {
    return asyncPost(
        INITIALIZE_TABLE_METRICS_CHECKPOINT,
        mapper.writeValueAsString(request),
        InitializeTableMetricsCheckpointResponse.class);
  }

  @SneakyThrows
  public CompletableFuture<GetTableMetricsCheckpointResponse> getTableMetricsCheckpoints(
      List<String> tableIds) {
    HttpUrl.Builder urlBuilder =
        HttpUrl.parse(ONEHOUSE_API_ENDPOINT + GET_TABLE_METRICS_CHECKPOINT).newBuilder();
    for (String tableId : tableIds) {
      urlBuilder.addQueryParameter("tableIds", tableId);
    }
    String url = urlBuilder.build().toString();
    return asyncGet(url, GetTableMetricsCheckpointResponse.class);
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

  @VisibleForTesting
  Headers getHeaders(OnehouseClientConfig onehouseClientConfig) {
    Headers.Builder headersBuilder = new Headers.Builder();
    headersBuilder.add(PROJECT_UID_KEY, onehouseClientConfig.getProjectId());
    headersBuilder.add(ONEHOUSE_API_KEY, onehouseClientConfig.getApiKey());
    headersBuilder.add(ONEHOUSE_API_SECRET_KEY, onehouseClientConfig.getApiSecret());
    headersBuilder.add(ONEHOUSE_USER_UUID_KEY, onehouseClientConfig.getUserId());
    if (StringUtils.isNotEmpty(onehouseClientConfig.getRequestId())) {
      headersBuilder.add(LINK_UID_KEY, onehouseClientConfig.getRequestId());
    }
    if (StringUtils.isNotEmpty(onehouseClientConfig.getRegion())) {
      headersBuilder.add(ONEHOUSE_REGION_KEY, onehouseClientConfig.getRegion());
    }
    if (onehouseClientConfig.getMaintenanceMode() != null && onehouseClientConfig.getMaintenanceMode().equals(Boolean.TRUE)) {
      headersBuilder.add(MAINTENANCE_MODE_KEY, Boolean.TRUE.toString());
    }
    return headersBuilder.build();
  }

  @VisibleForTesting
  <T> CompletableFuture<T> asyncGet(String url, Class<T> typeReference) {
    Request request = new Request.Builder().url(url).headers(headers).build();

    return asyncClient
        .makeRequestWithRetry(request)
        .thenApply(response -> handleResponse(response, typeReference));
  }

  @VisibleForTesting
  <T> CompletableFuture<T> asyncPost(String apiEndpoint, String json, Class<T> typeReference) {
    RequestBody body = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

    Request request =
        new Request.Builder()
            .url(ONEHOUSE_API_ENDPOINT + apiEndpoint)
            .post(body)
            .headers(headers)
            .build();

    return asyncClient
        .makeRequestWithRetry(request)
        .thenApply(response -> handleResponse(response, typeReference));
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
          if (response.code() == 401) {
            ((ApiResponse) errorResponse).setError(response.code(), UNAUTHORIZED_ERROR_MESSAGE);
          } else {
            ((ApiResponse) errorResponse).setError(response.code(), response.message());
          }
        }
        response.close();
        emmitApiErrorMetric(response.code());
        return errorResponse;
      } catch (InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        throw new RuntimeException("Failed to instantiate error response object", e);
      }
    }
  }

  private void emmitApiErrorMetric(int apiStatusCode) {
    if (ACCEPTABLE_HTTP_FAILURE_STATUS_CODES.contains(apiStatusCode)) {
      hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
          MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_USER_ERROR);
    } else {
      hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(
          MetricsConstants.MetadataUploadFailureReasons.API_FAILURE_SYSTEM_ERROR);
    }
  }
}
