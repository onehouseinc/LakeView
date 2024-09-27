package ai.onehouse.storage;

import static ai.onehouse.constants.MetadataExtractorConstants.DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.models.FileStreamData;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PresignedUrlFileUploaderTest {
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private final OkHttpClient client =
      new OkHttpClient.Builder()
          .dispatcher(new okhttp3.Dispatcher(executorService))
          .callTimeout(2, TimeUnit.SECONDS)
          .connectTimeout(2, TimeUnit.SECONDS)
          .readTimeout(2, TimeUnit.SECONDS)
          .writeTimeout(2, TimeUnit.SECONDS)
          .build();
  private final AsyncHttpClientWithRetry asyncHttpClientWithRetry =
      new AsyncHttpClientWithRetry(1, 1000L, client);
  @Mock AsyncStorageClient mockAsyncStorageClient;
  @Mock private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;
  private final String fileContent = "some-file-content";
  private final InputStream inputStream =
      IOUtils.toInputStream(fileContent, StandardCharsets.UTF_8);
  private MockWebServer mockWebServer;

  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final String FILE_URI = "s3://bucket/file";

  @BeforeEach
  void setup() {
    mockWebServer = new MockWebServer();
    when(mockAsyncStorageClient.streamFileAsync(FILE_URI))
        .thenReturn(
            CompletableFuture.completedFuture(
                FileStreamData.builder()
                    .inputStream(inputStream)
                    .fileSize(fileContent.length())
                    .build()));
  }

  @SneakyThrows
  private void setupMockWebServer(boolean isFailure) {
    if (isFailure) {
      mockWebServer.setDispatcher(
          new Dispatcher() {
            @Override
            public @NotNull MockResponse dispatch(@NotNull RecordedRequest recordedRequest) {
              return new MockResponse().setBody(FAILURE_ERROR).setResponseCode(FAILURE_STATUS_CODE);
            }
          });
    } else {
      mockWebServer.enqueue(new MockResponse().setBody(""));
    }
    mockWebServer.start();
  }

  @AfterEach
  void afterEach() throws IOException {
    mockWebServer.shutdown();
    executorService.shutdown();
  }

  @Test
  void testUploadFileToPresignedUrl() {
    setupMockWebServer(false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(
            mockWebServer.url("/upload").url().toString(),
            FILE_URI,
            DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
        .join();

    verify(mockAsyncStorageClient).streamFileAsync(FILE_URI);
    verifyRequestPayloadForSmallerFiles();
  }

  @Test
  void testUploadFileToPresignedUrlFailure() {
    setupMockWebServer(true);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () ->
                uploader
                    .uploadFileToPresignedUrl(
                        mockWebServer.url("/upload").url().toString(),
                        FILE_URI,
                        DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
                    .get());
    assertEquals(
        String.format(
            "ai.onehouse.exceptions.FileUploadException: File upload failed: response code: %s error message: Server Error",
            FAILURE_STATUS_CODE),
        exception.getMessage());
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            MetricsConstants.MetadataUploadFailureReasons.PRESIGNED_URL_UPLOAD_FAILURE);
    verifyRequestPayloadForSmallerFiles();
  }

  @Test
  void testUploadLargeFile() {
    setupMockWebServer(false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(mockWebServer.url("/upload").url().toString(), FILE_URI, 1)
        .join();

    verify(mockAsyncStorageClient).streamFileAsync(FILE_URI);
    verifyRequestPayload();
  }

  @SneakyThrows
  private void verifyRequestPayload() {
    RecordedRequest capturedRequest = mockWebServer.takeRequest(5, TimeUnit.SECONDS);

    assertNotNull(capturedRequest);
    assertEquals("application/octet-stream", capturedRequest.getHeader("content-type"));
    assertEquals(fileContent, capturedRequest.getBody().readUtf8());
    assertEquals("PUT", capturedRequest.getMethod());
  }

  @SneakyThrows
  private void verifyRequestPayloadForSmallerFiles() {
    RecordedRequest capturedRequest = mockWebServer.takeRequest(5, TimeUnit.SECONDS);

    assertNotNull(capturedRequest);
    assertEquals(fileContent, capturedRequest.getBody().readUtf8());
    assertEquals("PUT", capturedRequest.getMethod());
  }
}
