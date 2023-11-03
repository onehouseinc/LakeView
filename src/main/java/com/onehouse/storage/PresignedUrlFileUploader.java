package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.OkHttpResponseFuture;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PresignedUrlFileUploader {
  private final AsyncStorageClient asyncStorageClient;
  private final OkHttpClient okHttpClient;
  private static final Logger logger = LoggerFactory.getLogger(PresignedUrlFileUploader.class);

  @Inject
  public PresignedUrlFileUploader(
      @Nonnull AsyncStorageClient asyncStorageClient, @Nonnull OkHttpClient okHttpClient) {
    this.asyncStorageClient = asyncStorageClient;
    this.okHttpClient = okHttpClient;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String fileUrl) {
    logger.debug(String.format("Uploading %s to retrieved presigned url", presignedUrl));
    return asyncStorageClient
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
