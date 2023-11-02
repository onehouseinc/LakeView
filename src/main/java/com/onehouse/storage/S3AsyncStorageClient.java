package com.onehouse.storage;

import static com.onehouse.storage.StorageConstants.LIST_API_FILE_LIMIT;

import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class S3AsyncStorageClient implements AsyncStorageClient {
  private final S3AsyncClientProvider s3AsyncClientProvider;
  private final StorageUtils storageUtils;
  private static final Logger logger = LoggerFactory.getLogger(S3AsyncStorageClient.class);

  @Inject
  public S3AsyncStorageClient(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider, @Nonnull StorageUtils storageUtils) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
    this.storageUtils = storageUtils;
  }

  @Override
  public CompletableFuture<List<File>> listFiles(String s3Url) {
    logger.debug(String.format("Listing files in %s", s3Url));
    String BucketName = storageUtils.getS3BucketNameFromS3Url(s3Url);
    String prefix = storageUtils.getPathFromUrl(s3Url);

    // ensure prefix which is not the root dir always ends with "/"
    prefix = !Objects.equals(prefix, "") && !prefix.endsWith("/") ? prefix + "/" : prefix;
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(BucketName)
            .prefix(prefix)
            .maxKeys(LIST_API_FILE_LIMIT)
            .delimiter("/")
            .build();
    String finalPrefix = prefix;
    return s3AsyncClientProvider
        .getS3AsyncClient()
        .listObjectsV2(listObjectsV2Request)
        .thenApply(
            listObjectsV2Response -> {
              List<File> files =
                  listObjectsV2Response.contents().stream()
                      .map(
                          s3Object ->
                              File.builder()
                                  .filename(s3Object.key().replaceFirst(finalPrefix, ""))
                                  .lastModifiedAt(s3Object.lastModified())
                                  .isDirectory(false)
                                  .build())
                      .collect(Collectors.toList());
              files.addAll(
                  listObjectsV2Response.commonPrefixes().stream()
                      .map(
                          commonPrefix ->
                              File.builder()
                                  .filename(commonPrefix.prefix().replaceFirst(finalPrefix, ""))
                                  .isDirectory(true)
                                  .lastModifiedAt(Instant.EPOCH)
                                  .build())
                      .collect(Collectors.toList()));
              return files;
            });
  }

  @Override
  public CompletableFuture<InputStream> readFileAsInputStream(String s3Url) {
    return readFileFromS3(s3Url).thenApplyAsync(BytesWrapper::asInputStream);
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String s3Url) {
    return readFileFromS3(s3Url).thenApplyAsync(BytesWrapper::asByteArray);
  }

  private CompletableFuture<ResponseBytes<GetObjectResponse>> readFileFromS3(String s3Url) {
    logger.debug(String.format("Reading S3 file:  %s", s3Url));
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(storageUtils.getS3BucketNameFromS3Url(s3Url))
            .key(storageUtils.getPathFromUrl(s3Url))
            .build();

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes());
  }
}
