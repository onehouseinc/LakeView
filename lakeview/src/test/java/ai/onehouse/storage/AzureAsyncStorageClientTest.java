package ai.onehouse.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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
import com.azure.core.util.IterableStream;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class AzureAsyncStorageClientTest {

  @Mock private AzureStorageClientProvider mockAzureStorageClientProvider;
  @Mock private StorageUtils mockStorageUtils;
  @Mock private BlobServiceAsyncClient mockBlobServiceAsyncClient;
  @Mock private BlobContainerAsyncClient mockContainerAsyncClient;
  @Mock private BlobAsyncClient mockBlobAsyncClient;
  @Mock private PagedFlux<BlobItem> mockPagedFlux;
  @Mock private PagedResponse<BlobItem> mockPagedResponse1;
  @Mock private PagedResponse<BlobItem> mockPagedResponse2;
  @Mock private BlobItem mockBlobItem1;
  @Mock private BlobItem mockBlobItem2;
  @Mock private BlobItemProperties mockBlobItemProperties;

  private AzureAsyncStorageClient azureAsyncStorageClient;
  private static final String AZURE_URI =
      "https://testaccount.blob.core.windows.net/test-container/test-blob";
  private static final String TEST_CONTAINER = "test-container";
  private static final String TEST_BLOB = "test-blob";

  @BeforeEach
  void setup() {
    lenient()
        .when(mockAzureStorageClientProvider.getAzureAsyncClient())
        .thenReturn(mockBlobServiceAsyncClient);
    lenient().when(mockStorageUtils.getBucketNameFromUri(AZURE_URI)).thenReturn(TEST_CONTAINER);
    lenient().when(mockStorageUtils.getPathFromUrl(AZURE_URI)).thenReturn(TEST_BLOB);
    azureAsyncStorageClient =
        new AzureAsyncStorageClient(
            mockAzureStorageClientProvider, mockStorageUtils, ForkJoinPool.commonPool());
  }

  @Test
  void testListAllFilesInDir() throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String dirName = "dir1/";
    String continuationToken = "page_2";
    String prefix = TEST_BLOB + "/";

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.listBlobsByHierarchy(eq("/"), any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));
    when(mockPagedFlux.byPage(continuationToken)).thenReturn(Flux.just(mockPagedResponse2));

    // First page
    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockBlobItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(continuationToken);
    when(mockBlobItem1.getName()).thenReturn(prefix + fileName);
    when(mockBlobItem1.isPrefix()).thenReturn(null);
    when(mockBlobItem1.getProperties()).thenReturn(mockBlobItemProperties);
    when(mockBlobItemProperties.getLastModified()).thenReturn(OffsetDateTime.now());

    // Second page
    when(mockPagedResponse2.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockBlobItem2)));
    when(mockPagedResponse2.getContinuationToken()).thenReturn(null);
    when(mockBlobItem2.getName()).thenReturn(prefix + dirName);
    when(mockBlobItem2.isPrefix()).thenReturn(true);

    List<File> result = azureAsyncStorageClient.listAllFilesInDir(AZURE_URI).get();

    assertEquals(2, result.size());
    assertFalse(result.get(0).isDirectory());
    assertEquals(fileName, result.get(0).getFilename());
    assertTrue(result.get(1).isDirectory());
    assertEquals(dirName, result.get(1).getFilename());
  }

  @Test
  void testFetchObjectsByPage() throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String prefix = "prefix";
    String continuationToken = "token";
    String nextToken = "next-token";

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.listBlobsByHierarchy(eq("/"), any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage(continuationToken))
        .thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockBlobItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(nextToken);
    when(mockBlobItem1.getName()).thenReturn(prefix + "/" + fileName);
    when(mockBlobItem1.isPrefix()).thenReturn(null);
    when(mockBlobItem1.getProperties()).thenReturn(mockBlobItemProperties);
    when(mockBlobItemProperties.getLastModified()).thenReturn(OffsetDateTime.now());

    Pair<String, List<File>> result =
        azureAsyncStorageClient
            .fetchObjectsByPage(TEST_CONTAINER, prefix, continuationToken, null)
            .get();

    assertEquals(nextToken, result.getLeft());
    assertEquals(1, result.getRight().size());
    assertEquals("/" + fileName, result.getRight().get(0).getFilename());
  }

  @Test
  void testFetchObjectsByPageWithoutContinuationToken()
      throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String prefix = "prefix";

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.listBlobsByHierarchy(eq("/"), any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockBlobItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(null);
    when(mockBlobItem1.getName()).thenReturn(prefix + "/" + fileName);
    when(mockBlobItem1.isPrefix()).thenReturn(null);
    when(mockBlobItem1.getProperties()).thenReturn(mockBlobItemProperties);
    when(mockBlobItemProperties.getLastModified()).thenReturn(OffsetDateTime.now());

    Pair<String, List<File>> result =
        azureAsyncStorageClient.fetchObjectsByPage(TEST_CONTAINER, prefix, null, null).get();

    assertNull(result.getLeft());
    assertEquals(1, result.getRight().size());
  }

  @Test
  void testReadBlob() throws ExecutionException, InterruptedException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    BinaryData binaryData = BinaryData.fromBytes(fileContent);

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenReturn(Mono.just(binaryData));

    BinaryData result = azureAsyncStorageClient.readBlob(AZURE_URI).get();

    assertNotNull(result);
    assertArrayEquals(fileContent, result.toBytes());
  }

  @Test
  void testStreamFileAsync() throws ExecutionException, InterruptedException, IOException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    BinaryData binaryData = BinaryData.fromBytes(fileContent);

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenReturn(Mono.just(binaryData));

    FileStreamData result = azureAsyncStorageClient.streamFileAsync(AZURE_URI).get();

    assertNotNull(result);
    assertEquals(fileContent.length, result.getFileSize());

    byte[] resultContent = toByteArray(result.getInputStream());
    assertArrayEquals(fileContent, resultContent);
  }

  @Test
  void testReadFileAsBytes() throws ExecutionException, InterruptedException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    BinaryData binaryData = BinaryData.fromBytes(fileContent);

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenReturn(Mono.just(binaryData));

    byte[] result = azureAsyncStorageClient.readFileAsBytes(AZURE_URI).get();

    assertArrayEquals(fileContent, result);
  }

  @ParameterizedTest
  @MethodSource("generateBlobStorageExceptionTestCases")
  void testReadBlobWithBlobStorageException(
      BlobStorageException exception, Class<? extends RuntimeException> expectedExceptionClass) {
    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenAnswer(invocation -> Mono.error(exception));

    CompletableFuture<BinaryData> future = azureAsyncStorageClient.readBlob(AZURE_URI);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(expectedExceptionClass, cause);
  }

  static Stream<Arguments> generateBlobStorageExceptionTestCases() {
    BlobStorageException accessDenied403 = mock(BlobStorageException.class);
    when(accessDenied403.getStatusCode()).thenReturn(403);
    when(accessDenied403.getMessage()).thenReturn("Access denied");
    when(accessDenied403.getErrorCode()).thenReturn(null);

    BlobStorageException unauthorized401 = mock(BlobStorageException.class);
    when(unauthorized401.getStatusCode()).thenReturn(401);
    when(unauthorized401.getMessage()).thenReturn("Unauthorized");
    when(unauthorized401.getErrorCode()).thenReturn(null);

    BlobStorageException blobNotFound = mock(BlobStorageException.class);
    when(blobNotFound.getStatusCode()).thenReturn(404);
    when(blobNotFound.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
    when(blobNotFound.getMessage()).thenReturn("Blob not found");

    BlobStorageException containerNotFound = mock(BlobStorageException.class);
    when(containerNotFound.getStatusCode()).thenReturn(404);
    when(containerNotFound.getErrorCode()).thenReturn(BlobErrorCode.CONTAINER_NOT_FOUND);
    when(containerNotFound.getMessage()).thenReturn("Container not found");

    BlobStorageException tooManyRequests = mock(BlobStorageException.class);
    when(tooManyRequests.getStatusCode()).thenReturn(429);
    when(tooManyRequests.getMessage()).thenReturn("Too many requests");
    when(tooManyRequests.getErrorCode()).thenReturn(null);

    BlobStorageException serviceUnavailable = mock(BlobStorageException.class);
    when(serviceUnavailable.getStatusCode()).thenReturn(503);
    when(serviceUnavailable.getMessage()).thenReturn("Service unavailable");
    when(serviceUnavailable.getErrorCode()).thenReturn(null);

    BlobStorageException internalError = mock(BlobStorageException.class);
    when(internalError.getStatusCode()).thenReturn(500);
    when(internalError.getMessage()).thenReturn("Internal error");
    when(internalError.getErrorCode()).thenReturn(null);

    return Stream.of(
        Arguments.of(accessDenied403, AccessDeniedException.class),
        Arguments.of(unauthorized401, AccessDeniedException.class),
        Arguments.of(blobNotFound, NoSuchKeyException.class),
        Arguments.of(containerNotFound, NoSuchKeyException.class),
        Arguments.of(tooManyRequests, RateLimitException.class),
        Arguments.of(serviceUnavailable, RateLimitException.class),
        Arguments.of(internalError, ObjectStorageClientException.class));
  }

  @ParameterizedTest
  @MethodSource("generateWrappedExceptionTestCases")
  void testReadBlobWithWrappedException(
      RuntimeException exception, Class<? extends RuntimeException> expectedExceptionClass) {
    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenAnswer(invocation -> Mono.error(exception));

    CompletableFuture<BinaryData> future = azureAsyncStorageClient.readBlob(AZURE_URI);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(expectedExceptionClass, cause);
  }

  static Stream<Arguments> generateWrappedExceptionTestCases() {
    return Stream.of(
        Arguments.of(new AccessDeniedException("error"), AccessDeniedException.class),
        Arguments.of(new NoSuchKeyException("error"), NoSuchKeyException.class),
        Arguments.of(new RateLimitException("error"), RateLimitException.class));
  }

  @Test
  void testFetchObjectsByPageWithException() {
    BlobStorageException exception = mock(BlobStorageException.class);
    when(exception.getStatusCode()).thenReturn(429);
    when(exception.getMessage()).thenReturn("Throttled");
    when(exception.getErrorCode()).thenReturn(null);

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.listBlobsByHierarchy(eq("/"), any()))
        .thenThrow(exception);

    CompletableFuture<Pair<String, List<File>>> future =
        azureAsyncStorageClient.fetchObjectsByPage(TEST_CONTAINER, "prefix", null, null);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(RateLimitException.class, cause);
    assertTrue(
        cause.getMessage().contains("Throttled by Azure for operation: fetchObjectsByPage"));
  }

  @Test
  void testRefreshClient() {
    azureAsyncStorageClient.refreshClient();
    verify(mockAzureStorageClientProvider, times(1)).refreshClient();
  }

  @Test
  void testInitializeClient() {
    azureAsyncStorageClient.initializeClient();
    verify(mockAzureStorageClientProvider, times(1)).getAzureAsyncClient();
  }

  @Test
  void testReadBlobWithGenericException() {
    RuntimeException exception = new RuntimeException("Generic error");

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(TEST_BLOB)).thenReturn(mockBlobAsyncClient);
    when(mockBlobAsyncClient.downloadContent()).thenAnswer(invocation -> Mono.error(exception));

    CompletableFuture<BinaryData> future = azureAsyncStorageClient.readBlob(AZURE_URI);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(ObjectStorageClientException.class, cause);
  }

  @Test
  void testFetchObjectsByPageWithDirectoryItems() throws ExecutionException, InterruptedException {
    String dirName = "dir1/";
    String prefix = "prefix";

    when(mockBlobServiceAsyncClient.getBlobContainerAsyncClient(TEST_CONTAINER))
        .thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.listBlobsByHierarchy(eq("/"), any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockBlobItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(null);
    when(mockBlobItem1.getName()).thenReturn(prefix + dirName);
    when(mockBlobItem1.isPrefix()).thenReturn(true);

    Pair<String, List<File>> result =
        azureAsyncStorageClient.fetchObjectsByPage(TEST_CONTAINER, prefix, null, null).get();

    assertEquals(1, result.getRight().size());
    assertTrue(result.getRight().get(0).isDirectory());
    assertEquals(dirName, result.getRight().get(0).getFilename());
    assertEquals(Instant.EPOCH, result.getRight().get(0).getLastModifiedAt());
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
