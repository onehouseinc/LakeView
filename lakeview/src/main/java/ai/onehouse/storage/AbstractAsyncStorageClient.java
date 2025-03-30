package ai.onehouse.storage;

import ai.onehouse.exceptions.ObjectStorageClientException;
import ai.onehouse.storage.models.File;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAsyncStorageClient implements AsyncStorageClient {
  protected final ExecutorService executorService;
  protected final StorageUtils storageUtils;

  AbstractAsyncStorageClient(ExecutorService executorService, StorageUtils storageUtils) {
    this.executorService = executorService;
    this.storageUtils = storageUtils;
  }

  @Override
  public CompletableFuture<List<File>> listAllFilesInDir(String objectStorageUri) {
    log.debug("Listing files in {}", objectStorageUri);
    String bucketName = storageUtils.getBucketNameFromUri(objectStorageUri);
    String prefix = storageUtils.getPathFromUrl(objectStorageUri);

    // ensure prefix which is not the root dir always ends with "/"
    prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
    return listAllObjectsStorage(bucketName, prefix, null, new ArrayList<>());
  }

  protected CompletableFuture<List<File>> listAllObjectsStorage(
    String bucketName, String prefix, String continuationToken, List<File> files) {
    return fetchObjectsByPage(bucketName, prefix, continuationToken, null)
      .thenComposeAsync(
        continuationTokenAndFiles -> {
          String newContinuationToken = continuationTokenAndFiles.getLeft();
          files.addAll(continuationTokenAndFiles.getRight());
          if (newContinuationToken != null) {
            return listAllObjectsStorage(bucketName, prefix, newContinuationToken, files);
          } else {
            return CompletableFuture.completedFuture(files);
          }
        },
        executorService).exceptionally(throwable -> {
        log.error("Failed to list objects from storage", throwable);
        throw new ObjectStorageClientException(throwable);
      });
  }
}
