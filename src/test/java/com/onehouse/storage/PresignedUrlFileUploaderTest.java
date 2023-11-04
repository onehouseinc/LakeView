package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import org.junit.jupiter.api.Test;

class PresignedUrlFileUploaderTest {
  @Test
  void testUploadFileToPresignedUrl() throws ExecutionException, InterruptedException {
    String fileUri = "s3://bucket/file";
    String presignedUrl = "https://presigned-url";
    byte[] readFileResponse = new byte[] {};
    AsyncStorageClient mockAsyncStorageClient = mock(AsyncStorageClient.class);
    OkHttpClient mockOkHttpClient = mock(OkHttpClient.class);
    Call mockCall = mock(Call.class);

    when(mockAsyncStorageClient.readFileAsBytes(fileUri))
        .thenReturn(CompletableFuture.completedFuture(readFileResponse));
    when(mockOkHttpClient.newCall(any())).thenReturn(mockCall);

    Response fakeResponse =
        new Response.Builder()
            .request(new Request.Builder().url(presignedUrl).build())
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .build();

    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(0);
              callback.onResponse(mockCall, fakeResponse);
              return null;
            })
        .when(mockCall)
        .enqueue(any());

    PresignedUrlFileUploader uploader =
        new PresignedUrlFileUploader(mockAsyncStorageClient, mockOkHttpClient);

    uploader.uploadFileToPresignedUrl(presignedUrl, fileUri).get();

    verify(mockAsyncStorageClient).readFileAsBytes(fileUri);
    verify(mockOkHttpClient).newCall(any());
    verify(mockCall).enqueue(any());
  }
}
