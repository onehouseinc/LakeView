package com.onehouse.storage;

import static com.onehouse.constants.MetadataExtractorConstants.DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.constants.MetricsConstants;
import com.onehouse.metrics.LakeViewExtractorMetrics;
import com.onehouse.storage.models.FileStreamData;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PresignedUrlFileUploaderTest {
  @Mock private AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  @Mock AsyncStorageClient mockAsyncStorageClient;
  @Mock private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  @Mock private InputStream inputStream;

  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final String FILE_URI = "s3://bucket/file";
  private static final String PRESIGNED_URL = "https://presigned-url";
  private static final long LARGE_FILE_SIZE = 10 * 1024 * 1024; // 10MB

  @BeforeEach
  void setup() {
    when(mockAsyncStorageClient.streamFileAsync(FILE_URI))
        .thenReturn(
            CompletableFuture.completedFuture(
                FileStreamData.builder().inputStream(inputStream).fileSize(0).build()));
  }

  @Test
  void testUploadFileToPresignedUrl() throws ExecutionException, InterruptedException {
    mockOkHttpCall(PRESIGNED_URL, false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI, DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
        .get();

    verify(mockAsyncStorageClient).streamFileAsync(FILE_URI);
    verify(asyncHttpClientWithRetry).makeRequestWithRetry(any());
  }

  @Test
  void testUploadFileToPresignedUrlFailure() {
    mockOkHttpCall(PRESIGNED_URL, true);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () ->
                uploader
                    .uploadFileToPresignedUrl(
                        PRESIGNED_URL, FILE_URI, DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
                    .get());
    assertEquals(
        String.format(
            "java.lang.RuntimeException: File upload failed: response code: %s error message: %s",
            FAILURE_STATUS_CODE, FAILURE_ERROR),
        exception.getMessage());
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            MetricsConstants.MetadataUploadFailureReasons.PRESIGNED_URL_UPLOAD_FAILURE);
  }

  @Test
  void testUploadLargeFile() throws ExecutionException, InterruptedException, IOException {
    when(mockAsyncStorageClient.streamFileAsync(FILE_URI))
        .thenReturn(
            CompletableFuture.completedFuture(
                FileStreamData.builder()
                    .inputStream(inputStream)
                    .fileSize(LARGE_FILE_SIZE)
                    .build()));

    mockOkHttpCall(PRESIGNED_URL, false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI, DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
        .get();

    verify(mockAsyncStorageClient).streamFileAsync(FILE_URI);
    verify(asyncHttpClientWithRetry).makeRequestWithRetry(any());
  }

  @Test
  void testRequestBodyConstruction() throws IOException, ExecutionException, InterruptedException {
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    mockOkHttpCall(PRESIGNED_URL, false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI, DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
        .get();

    verify(asyncHttpClientWithRetry).makeRequestWithRetry(requestCaptor.capture());

    Request capturedRequest = requestCaptor.getValue();
    RequestBody capturedRequestBody = capturedRequest.body();

    assertNotNull(capturedRequestBody);
    assertEquals(MediaType.parse("application/octet-stream"), capturedRequestBody.contentType());
    assertEquals(0, capturedRequestBody.contentLength());
  }

  private void mockOkHttpCall(String url, boolean failure) {
    Response fakeResponse;
    ResponseBody responseBody = ResponseBody.create("", MediaType.get("text/plain"));
    if (failure) {
      fakeResponse =
          new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(FAILURE_STATUS_CODE)
              .message(FAILURE_ERROR)
              .body(responseBody)
              .build();
    } else {
      fakeResponse =
          new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(200)
              .message("OK")
              .body(responseBody)
              .build();
    }
    when(asyncHttpClientWithRetry.makeRequestWithRetry(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(fakeResponse));
  }
}
