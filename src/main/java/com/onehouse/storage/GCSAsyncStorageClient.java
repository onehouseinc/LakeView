package com.onehouse.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.GcsClientProvider;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
  public CompletableFuture<List<File>> listAllFilesInDir(String gcsUri) {
    return CompletableFuture.supplyAsync(
        () -> {
          log.debug("Listing files in {}", gcsUri);
          Bucket bucket =
              gcsClientProvider.getGcsClient().get(storageUtils.getBucketNameFromUri(gcsUri));
          String prefix = storageUtils.getPathFromUrl(gcsUri);

          // ensure prefix which is not the root dir always ends with "/"
          prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
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
                          .lastModifiedAt(
                              Instant.ofEpochMilli(!blob.isDirectory() ? blob.getUpdateTime() : 0))
                          .isDirectory(blob.isDirectory())
                          .build())
              .collect(Collectors.toList());
        },
        executorService);
  }

  @VisibleForTesting
  CompletableFuture<Blob> readBlob(String gcsUri) {
    log.debug("Reading GCS file: {}", gcsUri);
    return CompletableFuture.supplyAsync(
        () -> {
          Blob blob =
              gcsClientProvider
                  .getGcsClient()
                  .get(
                      BlobId.of(
                          storageUtils.getBucketNameFromUri(gcsUri),
                          storageUtils.getPathFromUrl(gcsUri)));
          if (blob != null) {
            return blob;
          } else {
            throw new RuntimeException("Blob not found");
          }
        },
        executorService);
  }

  @Override
  public CompletableFuture<InputStream> readFileAsInputStream(String gcsUri) {
    return readBlob(gcsUri).thenApply(blob -> Channels.newInputStream(blob.reader()));
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String gcsUri) {
    return readBlob(gcsUri).thenApply(Blob::getContent);
  }
}
