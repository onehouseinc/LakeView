package ai.onehouse.storage;

import static ai.onehouse.constants.MetadataExtractorConstants.DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.FileUploadException;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.models.FileStreamData;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
      spy(IOUtils.toInputStream(fileContent, StandardCharsets.UTF_8));
  private MockWebServer mockWebServer;

  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final String FILE_URI = "s3://bucket/file";

  @BeforeEach
  void setup() {
    mockWebServer = new MockWebServer();
  }

  private void stubStreamFile(InputStream is, long size) {
    when(mockAsyncStorageClient.streamFileAsync(FILE_URI))
        .thenReturn(
            CompletableFuture.completedFuture(
                FileStreamData.builder().inputStream(is).fileSize(size).build()));
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
    stubStreamFile(inputStream, fileContent.length());

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
    stubStreamFile(inputStream, fileContent.length());

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
            any(MetricsConstants.MetadataUploadFailureReasons.class),
            anyString());
    verifyRequestPayloadForSmallerFiles();
  }

  @Test
  void testUploadLargeFile() {
    setupMockWebServer(false);
    stubStreamFile(inputStream, fileContent.length());

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(mockWebServer.url("/upload").url().toString(), FILE_URI, 1)
        .join();

    verify(mockAsyncStorageClient).streamFileAsync(FILE_URI);
    verifyRequestPayload();
  }

  @Test
  @SneakyThrows
  void testUploadFileToPresignedUrl_smallFileClosesInputStreamOnSuccess() {
    setupMockWebServer(false);
    stubStreamFile(inputStream, fileContent.length());

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    uploader
        .uploadFileToPresignedUrl(
            mockWebServer.url("/upload").url().toString(),
            FILE_URI,
            DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
        .join();

    verify(inputStream).close();
  }

  @Test
  @SneakyThrows
  void testUploadFileToPresignedUrl_smallFileClosesInputStreamOnReadFailure() {
    setupMockWebServer(false);
    IOException simulatedError = new IOException("simulated read failure");
    InputStream throwingStream =
        spy(
            new InputStream() {
              @Override
              public int read() throws IOException {
                throw simulatedError;
              }

              @Override
              public int read(byte[] b, int off, int len) throws IOException {
                throw simulatedError;
              }
            });
    stubStreamFile(throwingStream, fileContent.length());

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, asyncHttpClientWithRetry, hudiMetadataExtractorMetrics);

    CompletionException thrown =
        assertThrows(
            CompletionException.class,
            () ->
                uploader
                    .uploadFileToPresignedUrl(
                        mockWebServer.url("/upload").url().toString(),
                        FILE_URI,
                        DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
                    .join());

    assertInstanceOf(FileUploadException.class, thrown.getCause());
    assertSame(simulatedError, thrown.getCause().getCause());
    verify(throwingStream).close();
  }

  @Test
  @SneakyThrows
  void testUploadFileToPresignedUrl_acceptable4xxNotRetried() {
    mockWebServer.setDispatcher(
        new Dispatcher() {
          @Override
          public @NotNull MockResponse dispatch(@NotNull RecordedRequest req) {
            return new MockResponse().setBody("forbidden").setResponseCode(403);
          }
        });
    mockWebServer.start();
    stubStreamFile(inputStream, fileContent.length());

    AsyncHttpClientWithRetry retryingClient = new AsyncHttpClientWithRetry(3, 50L, client);
    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(
            mockAsyncStorageClient, retryingClient, hudiMetadataExtractorMetrics);

    ExecutionException thrown =
        assertThrows(
            ExecutionException.class,
            () ->
                uploader
                    .uploadFileToPresignedUrl(
                        mockWebServer.url("/upload").url().toString(),
                        FILE_URI,
                        DEFAULT_FILE_UPLOAD_STREAM_BATCH_SIZE)
                    .get());

    assertInstanceOf(FileUploadException.class, thrown.getCause());
    assertEquals(
        1, mockWebServer.getRequestCount(), "acceptable 4xx status should not trigger retry");
    verify(hudiMetadataExtractorMetrics)
        .incrementTableMetadataProcessingFailureCounter(
            any(MetricsConstants.MetadataUploadFailureReasons.class), anyString());
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
