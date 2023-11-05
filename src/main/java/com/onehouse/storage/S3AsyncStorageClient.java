package com.onehouse.storage;

import com.google.inject.Inject;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

@Slf4j
public class S3AsyncStorageClient implements AsyncStorageClient {
  private final S3AsyncClientProvider s3AsyncClientProvider;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;

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
    log.debug("Listing files in {}", s3Uri);
    String bucketName = storageUtils.getBucketNameFromUri(s3Uri);
    String prefix = storageUtils.getPathFromUrl(s3Uri);

    // ensure prefix which is not the root dir always ends with "/"
    prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
    return listObjectsInS3(bucketName, prefix, null, new ArrayList<>());
  }

  private CompletableFuture<List<File>> listObjectsInS3(
      String bucketName, String prefix, String continuationToken, List<File> files) {

    ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).delimiter("/");

    if (continuationToken != null) {
      listObjectsV2RequestBuilder.continuationToken(continuationToken);
    }

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .listObjectsV2(listObjectsV2RequestBuilder.build())
        .thenComposeAsync(
            listObjectsV2Response -> {
              // process response
              files.addAll(processListObjectsV2Response(listObjectsV2Response, prefix));
              String newContinuationToken =
                  Boolean.TRUE.equals(listObjectsV2Response.isTruncated())
                      ? listObjectsV2Response.nextContinuationToken()
                      : null;
              if (newContinuationToken != null) {
                return listObjectsInS3(bucketName, prefix, newContinuationToken, files);
              } else {
                return CompletableFuture.completedFuture(files);
              }
            },
            executorService);
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
  public CompletableFuture<InputStream> readFileAsInputStream(String s3Uri) {
    return readFileFromS3(s3Uri).thenApplyAsync(BytesWrapper::asInputStream);
  }

  @Override
  public CompletableFuture<byte[]> readFileAsBytes(String s3Uri) {
    return readFileFromS3(s3Uri).thenApplyAsync(BytesWrapper::asByteArray);
  }

  private CompletableFuture<ResponseBytes<GetObjectResponse>> readFileFromS3(String s3Uri) {
    log.debug("Reading S3 file:  {}", s3Uri);
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(storageUtils.getBucketNameFromUri(s3Uri))
            .key(storageUtils.getPathFromUrl(s3Uri))
            .build();

    return s3AsyncClientProvider
        .getS3AsyncClient()
        .getObject(getObjectRequest, AsyncResponseTransformer.toBytes());
  }
}
