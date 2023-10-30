package com.onehouse.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.inject.Inject;
import com.onehouse.storage.providers.GcsClientProvider;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

public class GCSAsyncStorageReader implements AsyncStorageReader {
  private final GcsClientProvider gcsClientProvider;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;

  @Inject
  public GCSAsyncStorageReader(
      @Nonnull GcsClientProvider gcsClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    this.gcsClientProvider = gcsClientProvider;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
  }

  @Override
  public CompletableFuture<InputStream> readFile(String gcsUrl) {
    return CompletableFuture.supplyAsync(
        () -> {
          Blob blob =
              gcsClientProvider
                  .getGcsClient()
                  .get(
                      BlobId.of(
                          storageUtils.getGcsBucketNameFromPath(gcsUrl),
                          storageUtils.getPathFromUrl(gcsUrl)));
          if (blob != null) {
            return Channels.newInputStream(blob.reader());
          } else {
            throw new RuntimeException("Blob not found");
          }
        });
  }
}
