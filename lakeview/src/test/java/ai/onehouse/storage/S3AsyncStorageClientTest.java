package ai.onehouse.storage;

import static ai.onehouse.constants.MetricsConstants.MetadataUploadFailureReasons.ACCESS_DENIED;
import static ai.onehouse.constants.MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.ObjectStorageClientException;
import ai.onehouse.exceptions.RateLimitException;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
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
  void testStreamFileAsync() throws ExecutionException, InterruptedException, IOException {
    byte[] fileContent = "file content".getBytes(StandardCharsets.UTF_8);
    long contentLength = fileContent.length;

    stubStreamFileFromS3(fileContent, contentLength);

    FileStreamData result = s3AsyncStorageClient.streamFileAsync(S3_URI).get();
    InputStream resultInputStream = result.getInputStream();

    byte[] resultBytes = toByteArray(resultInputStream);
    assertArrayEquals(fileContent, resultBytes);
    assertEquals(contentLength, result.getFileSize());
  }

  @Test
  void testReadFileAsBytes() throws ExecutionException, InterruptedException {
    byte[] fileContent = "file content".getBytes(StandardCharsets.UTF_8);

    stubReadFileFromS3(fileContent);

    byte[] resultBytes = s3AsyncStorageClient.readFileAsBytes(S3_URI).get();

    assertArrayEquals(fileContent, resultBytes);
  }

  @ParameterizedTest
  @MethodSource("generateTestCases")
  void testStreamFileAsyncWithExceptions(String errorCode, Throwable throwable, String errorMessage, boolean sdkException) {
    when(mockS3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenReturn(sdkException ? buildSdkException(errorCode)  : buildS3Exception(errorCode));

    CompletableFuture<FileStreamData> streamFileAsync = s3AsyncStorageClient.streamFileAsync(S3_URI);
    CompletionException executionException = assertThrows(CompletionException.class, streamFileAsync::join);

    // Unwrap the exception to get to the root cause
    Throwable cause = executionException.getCause();

    // Verify the exception is RateLimitException
    assertInstanceOf(throwable.getClass(), cause);
    assertEquals(errorMessage, cause.getMessage());
  }

  static Stream<Arguments> generateTestCases() {
    return Stream.of(
        Arguments.of("Throttling",
            new RateLimitException("error"),
            String.format("Throttled by S3 for operation : streamFileAsync on path : s3://%s/%s",
                TEST_BUCKET, TEST_KEY), false),
        Arguments.of("AccessDenied",
            new AccessDeniedException("error"),
            String.format("AccessDenied for operation : streamFileAsync on path : s3://%s/%s with message : %s",
                TEST_BUCKET, TEST_KEY, "error"), false),
        Arguments.of("ExpiredToken",
            new AccessDeniedException("error"),
            String.format("AccessDenied for operation : streamFileAsync on path : s3://%s/%s with message : %s",
                TEST_BUCKET, TEST_KEY, "error"), false),
        Arguments.of("InternalError",
            new ObjectStorageClientException("error"),
            "java.util.concurrent.CompletionException: software.amazon.awssdk.awscore.exception.AwsServiceException: " +
                "error (Service: null, Status Code: 0, Request ID: null)", false),
        Arguments.of("Acquire operation took longer than the configured maximum time",
            new RateLimitException("Throttled by S3 (connection pool exhausted) for operation : streamFileAsync on path : s3://test-bucket/test-key"),
            "Throttled by S3 (connection pool exhausted) for operation : streamFileAsync on path : s3://test-bucket/test-key", true));
  }

  @ParameterizedTest
  @MethodSource("generateExceptionTestCases")
  void testReadFileAsBytesWithException(CompletableFuture<String> exceptionalFuture,
                                        Class<? extends RuntimeException> exceptionClass,
                                        String expectedMessage) {
    when(mockS3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenReturn(exceptionalFuture);

    CompletableFuture<byte[]> readFileAsBytes = s3AsyncStorageClient.readFileAsBytes(S3_URI);
    CompletionException executionException = assertThrows(CompletionException.class, readFileAsBytes::join);

    // Unwrap the exception to get to the root cause
    Throwable cause = executionException.getCause();

    // Verify the exception is RateLimitException
    assertInstanceOf(exceptionClass, cause);
    assertEquals(expectedMessage, cause.getMessage());
  }

  static Stream<Arguments> generateExceptionTestCases() {
    return Stream.of(
        Arguments.of(
        buildS3Exception("Throttling"),
        RateLimitException.class,
        "Throttled by S3 for operation : readFileAsBytes on path : s3://" + TEST_BUCKET + "/" + TEST_KEY),
        Arguments.of(buildNestedException(RATE_LIMITING),
            RateLimitException.class,
            RATE_LIMITING.name()),
        Arguments.of(buildNestedException(ACCESS_DENIED),
            AccessDeniedException.class,
            ACCESS_DENIED.name())
    );
  }

  @Test
  void testReadFileAsBytesWithRuntimeException() {
    CompletableFuture<GetObjectResponse> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(new RuntimeException("Error"));

    when(mockS3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenReturn(futureResponse);

    CompletableFuture<byte[]> readFileAsBytes = s3AsyncStorageClient.readFileAsBytes(S3_URI);
    CompletionException executionException = assertThrows(CompletionException.class, readFileAsBytes::join);

    // Unwrap the exception to get to the root cause
    Throwable cause = executionException.getCause();

    // Verify the exception is RateLimitException
    assertInstanceOf(RuntimeException.class, cause);
  }

  @MockitoSettings(strictness = Strictness.LENIENT)
  @Test
  void testFetchObjectsByPageWithS3RateLimiting() {
    when(mockS3AsyncClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(buildS3Exception("Throttling"));

    CompletableFuture<Pair<String, List<File>>> fetchObjects = s3AsyncStorageClient.fetchObjectsByPage(
            TEST_BUCKET,
            "prefix",
            "ct",
            "startAfter");
    CompletionException executionException = assertThrows(CompletionException.class, fetchObjects::join);

    // Unwrap the exception to get to the root cause
    Throwable cause = executionException.getCause();

    // Verify the exception is RateLimitException
    assertInstanceOf(RateLimitException.class, cause);
    assertEquals("Throttled by S3 for operation : fetchObjectsByPage on path : " + TEST_BUCKET,
            cause.getMessage());
  }

  private void stubStreamFileFromS3(byte[] fileContent, long contentLength) {
    GetObjectRequest expectedRequest =
        GetObjectRequest.builder().bucket(TEST_BUCKET).key(TEST_KEY).build();

    GetObjectResponse getObjectResponse =
        GetObjectResponse.builder().contentLength(contentLength).build();
    InputStream inputStream = new ByteArrayInputStream(fileContent);
    ResponseInputStream<GetObjectResponse> responseInputStream =
        new ResponseInputStream<>(getObjectResponse, inputStream);

    when(mockS3AsyncClient.getObject(eq(expectedRequest), any(AsyncResponseTransformer.class)))
        .thenReturn(CompletableFuture.completedFuture(responseInputStream));
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

  private static <R> CompletableFuture<R> buildS3Exception(String errorCode){

    CompletableFuture<R> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(AwsServiceException.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                    .errorCode(errorCode)
                    .errorMessage("error")
                    .build())
            .build());
    return futureResponse;
  }

  private static <R> CompletableFuture<R> buildSdkException(String message){
    CompletableFuture<R> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(SdkClientException.create(message));
    return futureResponse;
  }

  private static <R> CompletableFuture<R> buildNestedException(MetricsConstants.MetadataUploadFailureReasons type){
    RuntimeException runtimeException;
    switch (type) {
      case RATE_LIMITING:
        runtimeException = new RateLimitException(RATE_LIMITING.name());
        break;
      case ACCESS_DENIED:
        runtimeException = new AccessDeniedException(ACCESS_DENIED.name());
        break;
      default:
        runtimeException = new RuntimeException("SomeException");
    }
    CompletableFuture<R> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(runtimeException);
    return futureResponse;
  }
}
