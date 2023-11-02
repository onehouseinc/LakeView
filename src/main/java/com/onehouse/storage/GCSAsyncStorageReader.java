package com.onehouse.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.inject.Inject;
import com.onehouse.storage.providers.GcsClientProvider;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;

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
  public CompletableFuture<InputStream> readFileAsInputStream(String gcsUrl) {
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

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String gcsUrl) {
    return readFileAsInputStream(gcsUrl).thenApply(this::toByteArray);
  }

  @SneakyThrows
  private byte[] toByteArray(InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int read;
    while ((read = inputStream.read(buffer)) != -1) {
      baos.write(buffer, 0, read);
    }
    return baos.toByteArray();
  }
}
