package com.onehouse.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.GcsClientProvider;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;

public class GCSAsyncStorageClient implements AsyncStorageClient {
  private final GcsClientProvider gcsClientProvider;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;

  @Inject
  public GCSAsyncStorageClient(
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
          String prefix = storageUtils.getPathFromUrl(gcsUrl);

          // ensure prefix which is not the root dir always ends with "/"
          prefix = !Objects.equals(prefix, "") && !prefix.endsWith("/") ? prefix + "/" : prefix;
          Iterable<Blob> blobs =
              bucket
                  .list(
                      Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.delimiter("/"))
                  .iterateAll();
          String finalPrefix = prefix;
          return StreamSupport.stream(blobs.spliterator(), false)
              .map(
                  blob ->
                      File.builder()
                          .filename(blob.getName().replaceFirst(finalPrefix, ""))
                          .createdAt(
                              Instant.ofEpochMilli(!blob.isDirectory() ? blob.getCreateTime() : 0))
                          .isDirectory(blob.isDirectory())
                          .build())
              .collect(Collectors.toList());
        },
        executorService);
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
