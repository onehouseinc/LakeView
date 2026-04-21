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
import com.azure.storage.file.datalake.DataLakeDirectoryAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
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
            DataLakeServiceAsyncClient dataLakeServiceClient =
                azureStorageClientProvider.getAzureAsyncClient();
            DataLakeFileSystemAsyncClient fileSystemClient =
                dataLakeServiceClient.getFileSystemAsyncClient(containerName);

            ListPathsOptions options = new ListPathsOptions();
            if (StringUtils.isNotBlank(prefix)) {
              options.setPath(prefix);
            }

            PagedFlux<PathItem> pagedFlux = fileSystemClient.listPaths(options);

            List<File> files = new ArrayList<>();
            String nextContinuationToken = null;

            // Get single page with continuation token
            try (PagedResponse<PathItem> page =
                StringUtils.isNotBlank(continuationToken)
                    ? pagedFlux.byPage(continuationToken).blockFirst()
                    : pagedFlux.byPage().blockFirst()) {

              if (page != null) {
                // Process items in the page
                page.getElements()
                    .forEach(
                        pathItem -> {
                          String pathName = pathItem.getName();
                          boolean isDirectory = pathItem.isDirectory();
                          String fileName = pathName.replaceFirst("^" + prefix, "");

                          files.add(
                              File.builder()
                                  .filename(fileName)
                                  .lastModifiedAt(
                                      isDirectory
                                          ? Instant.EPOCH
                                          : pathItem.getLastModified().toInstant())
                                  .isDirectory(isDirectory)
                                  .build());
                        });

                // Get continuation token for next page
                nextContinuationToken = page.getContinuationToken();
              }
            }

            return Pair.of(nextContinuationToken, files);
          } catch (Exception ex) {
            return handleListPathsException(ex, containerName, prefix);
          }
        },
        executorService);
  }

  @VisibleForTesting
  CompletableFuture<BinaryData> readBlob(String azureUri) {
    log.debug("Reading Azure Data Lake file: {}", azureUri);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            DataLakeFileAsyncClient fileClient = getFileClient(azureUri);
            return BinaryData.fromFlux(fileClient.read()).block();
          } catch (Exception ex) {
            log.error("Failed to read file", ex);
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

  private DataLakeFileAsyncClient getFileClient(String azureUri) {
    String fileSystemName = storageUtils.getBucketNameFromUri(azureUri);
    String filePath = storageUtils.getPathFromUrl(azureUri);

    DataLakeServiceAsyncClient dataLakeServiceClient = azureStorageClientProvider.getAzureAsyncClient();
    DataLakeFileSystemAsyncClient fileSystemClient =
        dataLakeServiceClient.getFileSystemAsyncClient(fileSystemName);
    return fileSystemClient.getFileAsyncClient(filePath);
  }

  private Pair<String, List<File>> handleListPathsException(
      Exception ex, String containerName, String prefix) {
    // DataLake API returns 404 for non-existent paths, treat as empty directory
    Throwable wrappedException = ex.getCause() != null ? ex.getCause() : ex;
    if (wrappedException instanceof DataLakeStorageException) {
      DataLakeStorageException dlsException = (DataLakeStorageException) wrappedException;
      if ("PathNotFound".equals(dlsException.getErrorCode())
          || dlsException.getStatusCode() == 404) {
        log.debug(
            "Path not found, returning empty list for container: {}, prefix: {}",
            containerName,
            prefix);
        return Pair.of(null, new ArrayList<>());
      }
    }
    log.error("Failed to fetch objects by page", ex);
    throw clientException(ex, "fetchObjectsByPage", containerName);
  }

  @Override
  protected RuntimeException clientException(Throwable ex, String operation, String path) {
    Throwable wrappedException = ex.getCause() != null ? ex.getCause() : ex;

    if (wrappedException instanceof DataLakeStorageException) {
      DataLakeStorageException dataLakeException = (DataLakeStorageException) wrappedException;
      String errorCode = dataLakeException.getErrorCode();
      int statusCode = dataLakeException.getStatusCode();

      log.error(
          "Error in Azure Data Lake operation: {} on path: {} code: {} status: {} message: {}",
          operation,
          path,
          errorCode,
          statusCode,
          dataLakeException.getMessage());

      // Map to AccessDeniedException
      if (statusCode == 403 || statusCode == 401) {
        return new AccessDeniedException(
            String.format(
                "AccessDenied for operation: %s on path: %s with message: %s",
                operation, path, dataLakeException.getMessage()));
      }

      // Map to NoSuchKeyException
      if (errorCode != null
          && (errorCode.equals("PathNotFound")
              || errorCode.equals("FilesystemNotFound")
              || statusCode == 404)) {
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
