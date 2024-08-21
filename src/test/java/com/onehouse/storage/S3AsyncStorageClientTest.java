package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

@ExtendWith(MockitoExtension.class)
class S3AsyncStorageClientTest {
  @Mock private S3AsyncClientProvider mockS3AsyncClientProvider;
  @Mock private StorageUtils mockStorageUtils;
  @Mock private S3AsyncClient mockS3AsyncClient;
  private S3AsyncStorageClient s3AsyncStorageClient;
  private static final String S3_URI = "s3://test-bucket/test-key";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_KEY = "test-key";

  @BeforeEach
  void setup() {
    when(mockS3AsyncClientProvider.getS3AsyncClient()).thenReturn(mockS3AsyncClient);
    when(mockStorageUtils.getBucketNameFromUri(S3_URI)).thenReturn("test-bucket");
    when(mockStorageUtils.getPathFromUrl(S3_URI)).thenReturn("test-key");
    s3AsyncStorageClient =
        new S3AsyncStorageClient(
            mockS3AsyncClientProvider, mockStorageUtils, ForkJoinPool.commonPool());
  }

  @Test
  public void testListAllFilesInDir() throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String dirName = "dir1/";
    String continuationToken = "token";

    // simulating pagination
    ListObjectsV2Request expectedRequestPart1 =
        ListObjectsV2Request.builder()
            .bucket(TEST_BUCKET)
            .prefix(TEST_KEY + "/")
            .delimiter("/")
            .build();
    ListObjectsV2Request expectedRequestPart2 =
        expectedRequestPart1.toBuilder().continuationToken(continuationToken).build();

    ListObjectsV2Response listObjectsV2ResponsePart1 =
        ListObjectsV2Response.builder()
            .contents(
                S3Object.builder()
                    .key(TEST_KEY + "/" + fileName)
                    .lastModified(Instant.EPOCH)
                    .build())
            .isTruncated(true)
            .nextContinuationToken(continuationToken)
            .build();

    ListObjectsV2Response listObjectsV2ResponsePart2 =
        ListObjectsV2Response.builder()
            .commonPrefixes(CommonPrefix.builder().prefix(TEST_KEY + "/" + dirName).build())
            .isTruncated(false)
            .build();

    when(mockS3AsyncClient.listObjectsV2(expectedRequestPart1))
        .thenReturn(CompletableFuture.completedFuture(listObjectsV2ResponsePart1));
    when(mockS3AsyncClient.listObjectsV2(expectedRequestPart2))
        .thenReturn(CompletableFuture.completedFuture(listObjectsV2ResponsePart2));

    List<File> result = s3AsyncStorageClient.listAllFilesInDir(S3_URI).get();

    List<File> expectedFiles =
        Arrays.asList(
            File.builder()
                .isDirectory(false)
                .filename(fileName)
                .lastModifiedAt(Instant.EPOCH)
                .build(),
            File.builder()
                .isDirectory(true)
                .filename(dirName)
                .lastModifiedAt(Instant.EPOCH)
                .build());

    assertEquals(result, expectedFiles);
  }

  @Test
  void testReadFileAsInputStream() throws ExecutionException, InterruptedException, IOException {
    byte[] fileContent = "file content".getBytes(StandardCharsets.UTF_8);

    stubReadFileFromS3(fileContent);

    InputStream result = s3AsyncStorageClient.streamFileAsync(S3_URI).get().getInputStream();

    byte[] resultBytes = toByteArray(result);
    assertArrayEquals(fileContent, resultBytes);
  }

  @Test
  void testReadFileAsBytes() throws ExecutionException, InterruptedException, IOException {
    byte[] fileContent = "file content".getBytes(StandardCharsets.UTF_8);

    stubReadFileFromS3(fileContent);

    byte[] resultBytes = s3AsyncStorageClient.readFileAsBytes(S3_URI).get();

    assertArrayEquals(fileContent, resultBytes);
  }

  private void stubReadFileFromS3(byte[] fileContent) {
    GetObjectRequest expectedRequest =
        GetObjectRequest.builder().bucket(TEST_BUCKET).key(TEST_KEY).build();

    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseBytes<GetObjectResponse> responseBytes =
        ResponseBytes.fromByteArray(getObjectResponse, fileContent);

    when(mockS3AsyncClient.getObject(eq(expectedRequest), any(AsyncResponseTransformer.class)))
        .thenReturn(CompletableFuture.completedFuture(responseBytes));
  }

  private static byte[] toByteArray(InputStream is) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = is.read(buffer)) != -1) {
        baos.write(buffer, 0, bytesRead);
      }
      return baos.toByteArray();
    }
  }
}
