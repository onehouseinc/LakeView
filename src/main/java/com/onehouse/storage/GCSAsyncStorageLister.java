package com.onehouse.storage;

import static com.onehouse.storage.StorageConstants.GCS_PATH_PATTERN;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.GcsClientProvider;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

public class GCSAsyncStorageLister implements AsyncStorageLister {
  private final GcsClientProvider gcsClientProvider;
  private final ExecutorService executorService;

  @Inject
  public GCSAsyncStorageLister(
      @Nonnull ExecutorService executorService, @Nonnull GcsClientProvider gcsClientProvider) {
    this.gcsClientProvider = gcsClientProvider;
    this.executorService = executorService;
  }

  @Override
  public CompletableFuture<List<File>> listFiles(String gcsPath) {
    return CompletableFuture.supplyAsync(
        () -> {
          Bucket bucket = gcsClientProvider.getGcsClient().get(getGcsBucketNameFromPath(gcsPath));
          Iterable<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(gcsPath)).iterateAll();
          return StreamSupport.stream(blobs.spliterator(), false)
              .map(
                  blob ->
                      File.builder()
                          .filename(blob.getName())
                          .createdAt(Instant.ofEpochMilli(blob.getCreateTime()))
                          .isDirectory(blob.isDirectory())
                          .build())
              .collect(Collectors.toList());
        },
        executorService);
  }

  private String getGcsBucketNameFromPath(String gcsPath) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(gcsPath);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid GCS path: " + gcsPath);
  }
}
