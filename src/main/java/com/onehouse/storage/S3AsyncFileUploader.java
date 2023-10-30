package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.api.OkHttpResponseFuture;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3AsyncFileUploader {

  private final S3AsyncClientProvider s3AsyncClientProvider;
  private final OkHttpClient okHttpClient;
  private final S3Utils s3Utils;

  @Inject
  public S3AsyncFileUploader(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider,
      @Nonnull OkHttpClient okHttpClient,
      @Nonnull S3Utils s3Utils) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
    this.okHttpClient = okHttpClient;
    this.s3Utils = s3Utils;
  }

  public CompletableFuture<Void> uploadFileToPresignedUrl(String presignedUrl, String s3FileUrl) {

    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(s3Utils.getS3BucketNameFromS3Url(s3FileUrl))
            .key(s3Utils.getPathFromS3Url(s3FileUrl))
            .build();

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
        .thenComposeAsync(
            response -> {
              byte[] fileBytes = response.asByteArray();
              RequestBody requestBody = RequestBody.create(fileBytes);
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
