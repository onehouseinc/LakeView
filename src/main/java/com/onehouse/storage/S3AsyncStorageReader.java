package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3AsyncStorageReader implements AsyncStorageReader {
  private final S3AsyncClientProvider s3AsyncClientProvider;
  private final StorageUtils storageUtils;

  @Inject
  public S3AsyncStorageReader(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider, @Nonnull StorageUtils storageUtils) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
    this.storageUtils = storageUtils;
  }

  @Override
  public CompletableFuture<InputStream> readFile(String s3Path) {
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(storageUtils.getS3BucketNameFromS3Url(s3Path))
            .key(storageUtils.getPathFromUrl(s3Path))
            .build();

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
        .thenApplyAsync(BytesWrapper::asInputStream);
  }
}
