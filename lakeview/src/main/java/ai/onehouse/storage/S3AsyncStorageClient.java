package ai.onehouse.storage;

import ai.onehouse.exceptions.ObjectStorageClientException;
import ai.onehouse.exceptions.RateLimitException;
import com.google.inject.Inject;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.internal.AwsErrorCode;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

@Slf4j
public class S3AsyncStorageClient extends AbstractAsyncStorageClient {
  private final S3AsyncClientProvider s3AsyncClientProvider;

  @Inject
  public S3AsyncStorageClient(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    super(executorService, storageUtils);
    this.s3AsyncClientProvider = s3AsyncClientProvider;
  }

  @Override
  public CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String bucketName, String prefix, String continuationToken, String startAfter) {

    log.debug(
        "fetching files in dir {} continuationToken {} startAfter {}",
        prefix,
        continuationToken,
        startAfter);
    ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).delimiter("/");

    if (StringUtils.isNotBlank(startAfter)) {
      listObjectsV2RequestBuilder.startAfter(startAfter);
    }

    if (StringUtils.isNotBlank(continuationToken)) {
      listObjectsV2RequestBuilder.continuationToken(continuationToken);
    }

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .listObjectsV2(listObjectsV2RequestBuilder.build())
        .thenComposeAsync(
            listObjectsV2Response -> {
              // process response
              List<File> files = new ArrayList<>(processListObjectsV2Response(listObjectsV2Response, prefix));
              String newContinuationToken =
                  Boolean.TRUE.equals(listObjectsV2Response.isTruncated())
                      ? listObjectsV2Response.nextContinuationToken()
                      : null;
              return CompletableFuture.completedFuture(Pair.of(newContinuationToken, files));
            },
            executorService)
        .exceptionally(
            ex -> {
              log.error("Failed to fetch objects by page", ex);
              log.error("Error message {} and cause: ", ex.getMessage(), ex.getCause());
              throw clientException(ex, "fetchObjectsByPage", bucketName);
            }
        );
  }

  private List<File> processListObjectsV2Response(ListObjectsV2Response response, String prefix) {
    // process files
    List<File> files =
        response.contents().stream()
            .map(
                s3Object ->
                    File.builder()
                        .filename(s3Object.key().replaceFirst(prefix, ""))
                        .lastModifiedAt(s3Object.lastModified())
                        .isDirectory(false)
                        .build())
            .collect(Collectors.toList());
    // process directories
    files.addAll(
        response.commonPrefixes().stream()
            .map(
                commonPrefix ->
                    File.builder()
                        .filename(commonPrefix.prefix().replaceFirst(prefix, ""))
                        .isDirectory(true)
                        .lastModifiedAt(Instant.EPOCH)
                        .build())
            .collect(Collectors.toList()));

    return files;
  }

  @Override
  public CompletableFuture<FileStreamData> streamFileAsync(String s3Uri) {
    log.debug("Reading S3 file as InputStream: {}", s3Uri);
    GetObjectRequest getObjectRequest = getObjectRequest(s3Uri);
    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseResponseInputStream ->
                FileStreamData.builder()
                    .inputStream(responseResponseInputStream)
                    .fileSize(responseResponseInputStream.response().contentLength())
                    .build())
        .exceptionally(
                ex -> {
                  log.error("Failed to stream file", ex);
                  throw clientException(ex, "streamFileAsync", s3Uri);
                }
        );
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String s3Uri) {
    log.debug("Reading S3 file:  {}", s3Uri);
    GetObjectRequest getObjectRequest = getObjectRequest(s3Uri);
    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
        .thenApplyAsync(BytesWrapper::asByteArray)
        .exceptionally(
          ex -> {
            log.error("Failed to read file as bytes", ex);
            throw clientException(ex, "readFileAsBytes", s3Uri);
          }
        );
  }

  private GetObjectRequest getObjectRequest(String s3Uri) {
    return GetObjectRequest.builder()
        .bucket(storageUtils.getBucketNameFromUri(s3Uri))
        .key(storageUtils.getPathFromUrl(s3Uri))
        .build();
  }

  private RuntimeException clientException(Throwable ex, String operation, String path){
    Throwable wrappedException = ex.getCause();
    if (wrappedException instanceof AwsServiceException
        && AwsErrorCode.isThrottlingErrorCode(((AwsServiceException) wrappedException).awsErrorDetails().errorCode())){
        return new RateLimitException(String.format("Throttled by S3 for operation : %s on path : %s", operation, path));
    }
    return new ObjectStorageClientException(ex);
  }

  @Override
  public void refreshClient() {
    s3AsyncClientProvider.refreshClient();
  }
}
