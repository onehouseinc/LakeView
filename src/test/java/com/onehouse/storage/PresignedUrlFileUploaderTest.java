package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PresignedUrlFileUploaderTest {
  @Mock private Call mockCall;
  @Mock private OkHttpClient mockOkHttpClient;
  @Mock AsyncStorageClient mockAsyncStorageClient;
  private static final int FAILURE_STATUS_CODE = 500;
  private static final String FAILURE_ERROR = "call failed";
  private static final String FILE_URI = "s3://bucket/file";
  private static final String PRESIGNED_URL = "https://presigned-url";

  @BeforeEach
  void setup() {
    when(mockAsyncStorageClient.readFileAsBytes(FILE_URI))
        .thenReturn(CompletableFuture.completedFuture(new byte[] {}));
  }

  @Test
  void testUploadFileToPresignedUrl() throws ExecutionException, InterruptedException {
    mockOkHttpCall(PRESIGNED_URL, false);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(mockAsyncStorageClient, mockOkHttpClient);

    uploader.uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI).get();

    verify(mockAsyncStorageClient).readFileAsBytes(FILE_URI);
    verify(mockOkHttpClient).newCall(any());
    verify(mockCall).enqueue(any());
  }

  @Test
  void testUploadFileToPresignedUrlFailure() throws ExecutionException, InterruptedException {

    mockOkHttpCall(PRESIGNED_URL, true);

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(mockAsyncStorageClient, mockOkHttpClient);

    ExecutionException exception =
        assertThrows(
            ExecutionException.class,
            () -> uploader.uploadFileToPresignedUrl(PRESIGNED_URL, FILE_URI).get());
    assertEquals(
        String.format(
            "java.lang.RuntimeException: file upload failed failed: response code:  %d error message: %s",
            FAILURE_STATUS_CODE, FAILURE_ERROR),
        exception.getMessage());
  }

  private void mockOkHttpCall(String url, boolean failure) {
    when(mockOkHttpClient.newCall(any())).thenReturn(mockCall);
    Response fakeResponse;
    if (failure) {
      fakeResponse =
          new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(FAILURE_STATUS_CODE)
              .message(FAILURE_ERROR)
              .build();
    } else {
      fakeResponse =
          new Response.Builder()
              .request(new Request.Builder().url(url).build())
              .protocol(Protocol.HTTP_1_1)
              .code(200)
              .message("OK")
              .build();
    }
    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(0);
              callback.onResponse(mockCall, fakeResponse);
              return null;
            })
        .when(mockCall)
        .enqueue(any());
  }
}
