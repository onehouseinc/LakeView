package ai.onehouse.storage;

import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.NoSuchKeyException;
import ai.onehouse.exceptions.ObjectStorageClientException;
import ai.onehouse.exceptions.RateLimitException;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.AzureStorageClientProvider;
import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class AzureAsyncStorageClient extends AbstractAsyncStorageClient {
  private final AzureStorageClientProvider azureStorageClientProvider;

  @Inject
  public AzureAsyncStorageClient(
      @Nonnull AzureStorageClientProvider azureStorageClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    super(executorService, storageUtils);
    this.azureStorageClientProvider = azureStorageClientProvider;
  }

  @Override
  public CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String containerName, String prefix, String continuationToken, String startAfter) {

    log.debug(
        "fetching files in container {} with prefix {} continuationToken {} startAfter {}",
        containerName,
        prefix,
        continuationToken,
        startAfter);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            BlobServiceAsyncClient blobServiceClient =
                azureStorageClientProvider.getAzureAsyncClient();
            BlobContainerAsyncClient containerClient =
                blobServiceClient.getBlobContainerAsyncClient(containerName);

            ListBlobsOptions options = new ListBlobsOptions();
            if (StringUtils.isNotBlank(prefix)) {
              options.setPrefix(prefix);
            }

            PagedFlux<BlobItem> pagedFlux = containerClient.listBlobsByHierarchy("/", options);

            List<File> files = new ArrayList<>();
            String nextContinuationToken = null;

            // Get single page with continuation token
            PagedResponse<BlobItem> page =
                StringUtils.isNotBlank(continuationToken)
                    ? pagedFlux.byPage(continuationToken).blockFirst()
                    : pagedFlux.byPage().blockFirst();

            if (page != null) {
              // Process items in the page
              page.getElements()
                  .forEach(
                      blobItem -> {
                        String blobName = blobItem.getName();
                        boolean isDirectory = blobItem.isPrefix() != null && blobItem.isPrefix();
                        String fileName = blobName.replaceFirst("^" + prefix, "");

                        files.add(
                            File.builder()
                                .filename(fileName)
                                .lastModifiedAt(
                                    isDirectory
                                        ? Instant.EPOCH
                                        : blobItem.getProperties().getLastModified().toInstant())
                                .isDirectory(isDirectory)
                                .build());
                      });

              // Get continuation token for next page
              nextContinuationToken = page.getContinuationToken();
            }

            return Pair.of(nextContinuationToken, files);
          } catch (Exception ex) {
            log.error("Failed to fetch objects by page", ex);
            throw clientException(ex, "fetchObjectsByPage", containerName);
          }
        },
        executorService);
  }

  @VisibleForTesting
  CompletableFuture<BinaryData> readBlob(String azureUri) {
    log.debug("Reading Azure blob: {}", azureUri);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            BlobAsyncClient blobClient = getBlobClient(azureUri);
            return blobClient.downloadContent().block();
          } catch (Exception ex) {
            log.error("Failed to read blob", ex);
            throw clientException(ex, "readBlob", azureUri);
          }
        },
        executorService);
  }

  @Override
  public CompletableFuture<FileStreamData> streamFileAsync(String azureUri) {
    return readBlob(azureUri)
        .thenApply(
            binaryData ->
                FileStreamData.builder()
                    .inputStream(new ByteArrayInputStream(binaryData.toBytes()))
                    .fileSize((long) binaryData.toBytes().length)
                    .build());
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String azureUri) {
    return readBlob(azureUri).thenApply(BinaryData::toBytes);
  }

  private BlobAsyncClient getBlobClient(String azureUri) {
    String containerName = storageUtils.getBucketNameFromUri(azureUri);
    String blobPath = storageUtils.getPathFromUrl(azureUri);

    BlobServiceAsyncClient blobServiceClient = azureStorageClientProvider.getAzureAsyncClient();
    BlobContainerAsyncClient containerClient =
        blobServiceClient.getBlobContainerAsyncClient(containerName);
    return containerClient.getBlobAsyncClient(blobPath);
  }

  @Override
  protected RuntimeException clientException(Throwable ex, String operation, String path) {
    Throwable wrappedException = ex.getCause() != null ? ex.getCause() : ex;

    if (wrappedException instanceof BlobStorageException) {
      BlobStorageException blobException = (BlobStorageException) wrappedException;
      BlobErrorCode errorCode = blobException.getErrorCode();
      int statusCode = blobException.getStatusCode();

      log.error(
          "Error in Azure operation: {} on path: {} code: {} status: {} message: {}",
          operation,
          path,
          errorCode,
          statusCode,
          blobException.getMessage());

      // Map to AccessDeniedException
      if (statusCode == 403 || statusCode == 401) {
        return new AccessDeniedException(
            String.format(
                "AccessDenied for operation: %s on path: %s with message: %s",
                operation, path, blobException.getMessage()));
      }

      // Map to NoSuchKeyException
      if (errorCode == BlobErrorCode.BLOB_NOT_FOUND
          || errorCode == BlobErrorCode.CONTAINER_NOT_FOUND) {
        return new NoSuchKeyException(
            String.format("NoSuchKey for operation: %s on path: %s", operation, path));
      }

      // Map to RateLimitException
      if (statusCode == 429 || statusCode == 503) {
        return new RateLimitException(
            String.format("Throttled by Azure for operation: %s on path: %s", operation, path));
      }
    } else if (wrappedException instanceof AccessDeniedException
        || wrappedException instanceof NoSuchKeyException
        || wrappedException instanceof RateLimitException) {
      return (RuntimeException) wrappedException;
    }

    return new ObjectStorageClientException(ex);
  }

  @Override
  public void refreshClient() {
    azureStorageClientProvider.refreshClient();
  }

  @Override
  public void initializeClient() {
    azureStorageClientProvider.getAzureAsyncClient();
  }
}
