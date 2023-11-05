package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.OkHttpResponseFuture;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

@Slf4j
public class PresignedUrlFileUploader {
  private final AsyncStorageClient asyncStorageClient;
  private final OkHttpClient okHttpClient;

  @Inject
  public PresignedUrlFileUploader(
      @Nonnull AsyncStorageClient asyncStorageClient, @Nonnull OkHttpClient okHttpClient) {
    this.asyncStorageClient = asyncStorageClient;
    this.okHttpClient = okHttpClient;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
    log.debug("Uploading {} to retrieved presigned url", presignedUrl);
    return asyncStorageClient
        .readFileAsBytes(fileUrl)
        .thenComposeAsync(
            response -> {
              RequestBody requestBody = RequestBody.create(response);
              Request request;
              request = new Request.Builder().url(presignedUrl).put(requestBody).build();

              OkHttpResponseFuture callback = new OkHttpResponseFuture();
              okHttpClient.newCall(request).enqueue(callback);

              return callback.future.thenApply(
                  uploadResponse -> {
                    if (!uploadResponse.isSuccessful()) {
                      throw new RuntimeException(
                          String.format(
                              "file upload failed failed: response code:  %s error message: %s",
                              uploadResponse.code(), uploadResponse.message()));
                    }
                    return null; // Successfully uploaded
                  });
            });
  }
}
