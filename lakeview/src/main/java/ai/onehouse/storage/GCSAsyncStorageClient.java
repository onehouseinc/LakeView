package ai.onehouse.storage;

import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.ObjectStorageClientException;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.GcsClientProvider;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class GCSAsyncStorageClient extends AbstractAsyncStorageClient {
  private final GcsClientProvider gcsClientProvider;

  @Inject
  public GCSAsyncStorageClient(
      @Nonnull GcsClientProvider gcsClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    super(executorService, storageUtils);
    this.gcsClientProvider = gcsClientProvider;
  }

  @Override
  public CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String bucketName, String prefix, String continuationToken, String startAfter) {
    log.debug(
        "fetching files in dir {} continuationToken {} startAfter {}",
        prefix,
        continuationToken,
        startAfter);
    return CompletableFuture.supplyAsync(
        () -> {
          List<Storage.BlobListOption> optionList =
              new ArrayList<>(
                  Arrays.asList(
                      Storage.BlobListOption.prefix(prefix),
                      Storage.BlobListOption.delimiter("/")));
          if (StringUtils.isNotBlank(continuationToken)) {
            optionList.add(Storage.BlobListOption.pageToken(continuationToken));
          }
          if (StringUtils.isNotBlank(startAfter)) {
            optionList.add(Storage.BlobListOption.startOffset(startAfter));
          }
          Page<Blob> blobs =
              gcsClientProvider
                  .getGcsClient()
                  .list(bucketName, optionList.toArray(new Storage.BlobListOption[0]));
          List<File> files = new ArrayList<>();
          for (Blob blob : blobs.getValues()) {
            files.add(
                File.builder()
                    .filename(blob.getName().replaceFirst(prefix, ""))
                    .lastModifiedAt(
                        Instant.ofEpochMilli(!blob.isDirectory() ? blob.getUpdateTime() : 0))
                    .isDirectory(blob.isDirectory())
                    .build());
          }
          String nextPageToken = blobs.hasNextPage() ? blobs.getNextPageToken() : null;
          return Pair.of(nextPageToken, files);
        },
      executorService).exceptionally(
      ex -> {
        log.error("Failed to fetch objects by page", ex);
        throw clientException(ex, "fetchObjectsByPage", bucketName);
      }
    );
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
            throw new ObjectStorageClientException("Blob not found");
          }
        },
        executorService).exceptionally(
      ex -> {
        log.error("Failed to read blob", ex);
        throw clientException(ex, "readBlob", gcsUri);
      }
    );
  }

  @Override
  public CompletableFuture<FileStreamData> streamFileAsync(String gcsUri) {
    return readBlob(gcsUri)
        .thenApply(
            blob ->
                FileStreamData.builder()
                    .inputStream(Channels.newInputStream(blob.reader()))
                    .fileSize(blob.getSize())
                    .build());
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String gcsUri) {
    return readBlob(gcsUri).thenApply(Blob::getContent);
  }

  @Override
  public void refreshClient() {
    gcsClientProvider.refreshClient();
  }

  @Override
  public void initializeClient() {
    gcsClientProvider.getGcsClient();
  }

  @Override
  protected RuntimeException clientException(Throwable ex, String operation, String path) {
    Throwable wrappedException = ex.getCause();
    if (wrappedException instanceof StorageException) {
      StorageException storageException = (StorageException) wrappedException;
      log.info("Error in GCS operation : {} on path : {} code : {} message : {}",
          operation, path, storageException.getCode(), storageException.getMessage());
      if (storageException.getCode() == 403 || storageException.getCode() == 401
          || storageException.getMessage().equalsIgnoreCase("Error requesting access token")) {
        return new AccessDeniedException(
            String.format(
                "AccessDenied for operation : %s on path : %s with message : %s",
                operation, path, storageException.getMessage()));
      }
    } else if (wrappedException instanceof  AccessDeniedException) {
      return (RuntimeException) wrappedException;
    }
    return new ObjectStorageClientException(ex);
  }
}
