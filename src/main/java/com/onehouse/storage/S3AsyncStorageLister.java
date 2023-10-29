package com.onehouse.storage;

import static com.onehouse.storage.S3Utils.getPathFromS3Url;
import static com.onehouse.storage.S3Utils.getS3BucketNameFromS3Url;

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

  @Inject
  public S3AsyncStorageLister(@Nonnull S3AsyncClientProvider s3AsyncClientProvider) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
  }

  @Override
  public CompletableFuture<List<File>> listFiles(String s3path) {
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(getS3BucketNameFromS3Url(s3path))
            .prefix(getPathFromS3Url(s3path))
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
                                .build())
                    .collect(Collectors.toList()));
  }
}
