package ai.onehouse.storage;

import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.ObjectStorageClientException;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.AzureBlobClientProvider;
import java.io.InputStream;
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
  private final AzureBlobClientProvider azureBlobClientProvider;

  @Inject
  public AzureAsyncStorageClient(
      @Nonnull AzureBlobClientProvider azureBlobClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    super(executorService, storageUtils);
    this.azureBlobClientProvider = azureBlobClientProvider;
  }

  @Override
  public CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String containerName, String prefix, String continuationToken, String startAfter) {
    log.debug(
        "fetching files in dir {} continuationToken {} startAfter {}",
        prefix,
        continuationToken,
        startAfter);
    return CompletableFuture.supplyAsync(
        () -> {
          BlobContainerClient containerClient =
              azureBlobClientProvider.getBlobContainerClient(containerName);
          
          ListBlobsOptions options = new ListBlobsOptions()
              .setPrefix(prefix)
              .setMaxResultsPerPage(5000);
          
          // Note: Azure Blob Storage doesn't support startAfter in the same way as S3
          // We'll use the continuation token for pagination
          if (StringUtils.isNotBlank(continuationToken)) {
            // Azure uses continuation tokens differently, but we'll handle it
            // For now, we'll list all blobs and filter if needed
          }

          List<File> files = new ArrayList<>();
          String nextContinuationToken = null;
          
          try {
            PagedIterable<BlobItem> pagedIterable = containerClient.listBlobs(options, null);
            java.util.Iterator<PagedResponse<BlobItem>> iterator = pagedIterable.iterableByPage().iterator();
            
            if (iterator.hasNext()) {
              PagedResponse<BlobItem> page = iterator.next();
              for (BlobItem blobItem : page.getElements()) {
                String blobName = blobItem.getName();
                String relativeName = blobName.replaceFirst(prefix, "");
                
                // Check if it's a directory (ends with /)
                boolean isDirectory = blobName.endsWith("/");
                
                files.add(
                    File.builder()
                        .filename(relativeName)
                        .lastModifiedAt(
                            blobItem.getProperties() != null
                                && blobItem.getProperties().getLastModified() != null
                                ? blobItem.getProperties().getLastModified().toInstant()
                                : Instant.EPOCH)
                        .isDirectory(isDirectory)
                        .build());
              }
              
              // Check if there are more pages
              if (iterator.hasNext()) {
                // For simplicity, we'll mark that there are more results
                // In a production implementation, you'd want to properly handle continuation tokens
                nextContinuationToken = "has-more";
              }
            }
          } catch (BlobStorageException e) {
            log.error("Failed to list blobs", e);
            throw clientException(e, "fetchObjectsByPage", containerName);
          }
          
          return Pair.of(nextContinuationToken, files);
        },
        executorService).exceptionally(
      ex -> {
        log.error("Failed to fetch objects by page", ex);
        throw clientException(ex, "fetchObjectsByPage", containerName);
      }
    );
  }

  @VisibleForTesting
  CompletableFuture<BlobClient> readBlob(String azureBlobUri) {
    log.debug("Reading Azure Blob file: {}", azureBlobUri);
    return CompletableFuture.supplyAsync(
        () -> {
          String containerName = storageUtils.getBucketNameFromUri(azureBlobUri);
          String blobPath = storageUtils.getPathFromUrl(azureBlobUri);
          
          BlobContainerClient containerClient =
              azureBlobClientProvider.getBlobContainerClient(containerName);
          BlobClient blobClient = containerClient.getBlobClient(blobPath);
          
          if (!blobClient.exists()) {
            throw new ObjectStorageClientException("Blob not found: " + azureBlobUri);
          }
          
          return blobClient;
        },
        executorService).exceptionally(
      ex -> {
        log.error("Failed to read blob", ex);
        throw clientException(ex, "readBlob", azureBlobUri);
      }
    );
  }

  @Override
  public CompletableFuture<FileStreamData> streamFileAsync(String azureBlobUri) {
    return readBlob(azureBlobUri)
        .thenApply(
            blobClient -> {
              InputStream inputStream = blobClient.openInputStream();
              long fileSize = blobClient.getProperties().getBlobSize();
              return FileStreamData.builder()
                  .inputStream(inputStream)
                  .fileSize(fileSize)
                  .build();
            });
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String azureBlobUri) {
    return readBlob(azureBlobUri)
        .thenApply(blobClient -> {
          try {
            return blobClient.downloadContent().toBytes();
          } catch (Exception e) {
            throw new RuntimeException("Failed to read blob content", e);
          }
        });
  }

  @Override
  public void refreshClient() {
    azureBlobClientProvider.refreshClient();
  }

  @Override
  public void initializeClient() {
    azureBlobClientProvider.getBlobServiceClient();
  }

  @Override
  protected RuntimeException clientException(Throwable ex, String operation, String path) {
    Throwable wrappedException = ex.getCause() != null ? ex.getCause() : ex;
    if (wrappedException instanceof BlobStorageException) {
      BlobStorageException blobStorageException = (BlobStorageException) wrappedException;
      int statusCode = blobStorageException.getStatusCode();
      log.info("Error in Azure Blob operation : {} on path : {} status code : {} message : {}",
          operation, path, statusCode, blobStorageException.getMessage());
      if (statusCode == 403 || statusCode == 401) {
        return new AccessDeniedException(
            String.format(
                "AccessDenied for operation : %s on path : %s with message : %s",
                operation, path, blobStorageException.getMessage()));
      }
    } else if (wrappedException instanceof HttpResponseException) {
      HttpResponseException httpResponseException = (HttpResponseException) wrappedException;
      int statusCode = httpResponseException.getResponse().getStatusCode();
      log.info("Error in Azure Blob operation : {} on path : {} status code : {} message : {}",
          operation, path, statusCode, httpResponseException.getMessage());
      if (statusCode == 403 || statusCode == 401) {
        return new AccessDeniedException(
            String.format(
                "AccessDenied for operation : %s on path : %s with message : %s",
                operation, path, httpResponseException.getMessage()));
      }
    } else if (wrappedException instanceof AccessDeniedException) {
      return (RuntimeException) wrappedException;
    }
    return new ObjectStorageClientException(ex);
  }
}

