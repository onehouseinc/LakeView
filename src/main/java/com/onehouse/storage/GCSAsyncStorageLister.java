package com.onehouse.storage;

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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

public class GCSAsyncStorageLister implements AsyncStorageLister {
  private final GcsClientProvider gcsClientProvider;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;

  @Inject
  public GCSAsyncStorageLister(
      @Nonnull GcsClientProvider gcsClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    this.gcsClientProvider = gcsClientProvider;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
  }

  @Override
  public CompletableFuture<List<File>> listFiles(String gcsUrl) {
    return CompletableFuture.supplyAsync(
        () -> {
          Bucket bucket =
              gcsClientProvider.getGcsClient().get(storageUtils.getGcsBucketNameFromPath(gcsUrl));
          Iterable<Blob> blobs =
              bucket
                  .list(Storage.BlobListOption.prefix(storageUtils.getPathFromUrl(gcsUrl)))
                  .iterateAll();
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
}
