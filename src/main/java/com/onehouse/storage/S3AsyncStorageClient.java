package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
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
  private final ExecutorService executorService;
  private static final Logger LOGGER = LoggerFactory.getLogger(S3AsyncStorageClient.class);

  @Inject
  public S3AsyncStorageClient(
      @Nonnull S3AsyncClientProvider s3AsyncClientProvider,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ExecutorService executorService) {
    this.s3AsyncClientProvider = s3AsyncClientProvider;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
  }

  @Override
  public CompletableFuture<List<File>> listAllFilesInDir(String s3Uri) {
    LOGGER.debug(String.format("Listing files in %s", s3Uri));
    String bucketName = storageUtils.getS3BucketNameFromS3Url(s3Uri);
    String prefix = storageUtils.getPathFromUrl(s3Uri);

    // ensure prefix which is not the root dir always ends with "/"
    prefix = !Objects.equals(prefix, "") && !prefix.endsWith("/") ? prefix + "/" : prefix;

    List<File> files = new java.util.ArrayList<>(List.of());

    CompletableFuture<Pair<String, List<File>>> listPaginatedObjectsFuture =
        listObjectsInS3(prefix, bucketName, null);
    AtomicBoolean hasMore = new AtomicBoolean(true);
    while (hasMore.get()) {
      String finalPrefix = prefix;
      listPaginatedObjectsFuture =
          listPaginatedObjectsFuture.thenComposeAsync(
              continuationTokenFileListPair -> {
                // TODO: verify assumption that continuationToken field is null when all files are
                // listed
                String continuationToken = continuationTokenFileListPair.getLeft();
                hasMore.set(!continuationToken.isBlank());
                files.addAll(continuationTokenFileListPair.getRight());
                return listObjectsInS3(finalPrefix, bucketName, continuationToken);
              },
              executorService);
    }

    return listPaginatedObjectsFuture.thenApply(Pair::getRight);
  }

  private CompletableFuture<Pair<String, List<File>>> listObjectsInS3(
      String bucketName, String prefix, String continuationToken) {

    ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).delimiter("/");

    if (continuationToken != null) {
      listObjectsV2RequestBuilder.continuationToken(continuationToken);
    }

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .listObjectsV2(listObjectsV2RequestBuilder.build())
        .thenApplyAsync(
            listObjectsV2Response -> {
              List<File> files =
                  listObjectsV2Response.contents().stream()
                      .map(
                          s3Object ->
                              File.builder()
                                  .filename(s3Object.key().replaceFirst(prefix, ""))
                                  .lastModifiedAt(s3Object.lastModified())
                                  .isDirectory(false)
                                  .build())
                      .collect(Collectors.toList());
              files.addAll(
                  listObjectsV2Response.commonPrefixes().stream()
                      .map(
                          commonPrefix ->
                              File.builder()
                                  .filename(commonPrefix.prefix().replaceFirst(prefix, ""))
                                  .isDirectory(true)
                                  .lastModifiedAt(Instant.EPOCH)
                                  .build())
                      .collect(Collectors.toList()));
              return Pair.of(listObjectsV2Response.continuationToken(), files);
            },
            executorService);
  }

  @Override
  public CompletableFuture<InputStream> readFileAsInputStream(String s3Uri) {
    return readFileFromS3(s3Uri).thenApplyAsync(BytesWrapper::asInputStream);
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String s3Uri) {
    return readFileFromS3(s3Uri).thenApplyAsync(BytesWrapper::asByteArray);
  }

  private CompletableFuture<ResponseBytes<GetObjectResponse>> readFileFromS3(String s3Uri) {
    LOGGER.debug(String.format("Reading S3 file:  %s", s3Uri));
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(storageUtils.getS3BucketNameFromS3Url(s3Uri))
            .key(storageUtils.getPathFromUrl(s3Uri))
            .build();

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes());
  }
}
