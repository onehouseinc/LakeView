package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class S3AsyncStorageLister implements AsyncStorageLister {
  private final S3AsyncClientProvider s3AsyncClientProvider;
  private final StorageUtils storageUtils;

  @Inject
  public S3AsyncStorageLister(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider, @Nonnull StorageUtils storageUtils) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
    this.storageUtils = storageUtils;
  }

  @Override
  public CompletableFuture<List<File>> listFiles(String s3path) {
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(storageUtils.getS3BucketNameFromS3Url(s3path))
            .prefix(storageUtils.getPathFromUrl(s3path))
            .build();
    return s3AsyncClientProvider
        .getS3AsyncClient()
        .listObjectsV2(listObjectsV2Request)
        .thenApply(
            listObjectsV2Response ->
                listObjectsV2Response.contents().stream()
                    .map(
                        s3Object ->
                            File.builder()
                                .filename(s3Object.key())
                                .createdAt(s3Object.lastModified())
                                .isDirectory(s3Object.key().endsWith("/"))
                                .build())
                    .collect(Collectors.toList()));
  }
}
