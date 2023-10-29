package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
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

              CompletableFuture<Void> future = new CompletableFuture<>();

              okHttpClient
                  .newCall(request)
                  .enqueue(
                      new Callback() {
                        @Override
                        public void onFailure(Call call, IOException e) {
                          future.completeExceptionally(e);
                        }

                        @Override
                        public void onResponse(Call call, Response response) throws IOException {
                          if (!response.isSuccessful()) {
                            future.completeExceptionally(
                                new RuntimeException(
                                    "Failed to upload file: "
                                        + response.code()
                                        + " "
                                        + response.message()));
                          } else {
                            future.complete(null); // Successfully uploaded
                          }
                        }
                      });

              return future;
            });
  }
}
