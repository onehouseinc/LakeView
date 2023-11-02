package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.OkHttpResponseFuture;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class PresignedUrlFileUploader {

  private final AsyncStorageReader asyncStorageReader;
  private final OkHttpClient okHttpClient;

  @Inject
  public PresignedUrlFileUploader(
      @Nonnull AsyncStorageReader asyncStorageReader, @Nonnull OkHttpClient okHttpClient) {
    this.asyncStorageReader = asyncStorageReader;
    this.okHttpClient = okHttpClient;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
    return asyncStorageReader
        .readFileAsBytes(fileUrl)
        .thenComposeAsync(
            response -> {
              RequestBody requestBody = RequestBody.create(response);
              Request request = new Request.Builder().url(presignedUrl).put(requestBody).build();

              OkHttpResponseFuture callback = new OkHttpResponseFuture();
              okHttpClient.newCall(request).enqueue(callback);

              return callback.future.thenApply(
                  uploadResponse -> {
                    if (!uploadResponse.isSuccessful()) {
                      throw new RuntimeException(
                          "file upload failed failed: "
                              + uploadResponse.code()
                              + " "
                              + uploadResponse.message());
                    }
                    return null; // Successfully uploaded
                  });
            });
  }
}
