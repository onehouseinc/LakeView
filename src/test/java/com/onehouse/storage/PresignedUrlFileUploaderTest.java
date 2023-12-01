package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.onehouse.api.AsyncHttpClientWithRetry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PresignedUrlFileUploaderTest {
  @Mock private AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  @Mock AsyncStorageClient mockAsyncStorageClient;
  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final String FILE_URI = "s3://bucket/file";
  private static final String PRESIGNED_URL = "https://presigned-url";
  private static final byte[] FILE_CONTENT = new byte[] {};

  @BeforeEach
  void setup() {
    when(mockAsyncStorageClient.readFileAsBytes(FILE_URI))
        .thenReturn(CompletableFuture.completedFuture(FILE_CONTENT));
  }

  @Test
  void testUploadFileToPresignedUrl() throws ExecutionException, InterruptedException {
    mockOkHttpCall(PRESIGNED_URL, false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(mockAsyncStorageClient, asyncHttpClientWithRetry);

    uploader.uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI).get();

    verify(mockAsyncStorageClient).readFileAsBytes(FILE_URI);
    verify(asyncHttpClientWithRetry).makeRequestWithRetry(any());
  }

  @Test
  void testUploadFileToPresignedUrlFailure() {

    mockOkHttpCall(PRESIGNED_URL, true);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(mockAsyncStorageClient, asyncHttpClientWithRetry);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () -> uploader.uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI).get());
    assertEquals(
        String.format(
            "java.lang.RuntimeException: file upload failed failed: response code: %d error message: %s",
            FAILURE_STATUS_CODE, FAILURE_ERROR),
        exception.getMessage());
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
