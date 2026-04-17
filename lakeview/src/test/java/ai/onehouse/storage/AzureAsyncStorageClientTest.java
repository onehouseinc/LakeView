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
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.core.util.IterableStream;
import com.azure.storage.file.datalake.models.PathItem;
import java.nio.ByteBuffer;
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
  @Mock private DataLakeServiceAsyncClient mockDataLakeServiceAsyncClient;
  @Mock private DataLakeFileSystemAsyncClient mockFileSystemAsyncClient;
  @Mock private DataLakeFileAsyncClient mockFileAsyncClient;
  @Mock private PagedFlux<PathItem> mockPagedFlux;
  @Mock private PagedResponse<PathItem> mockPagedResponse1;
  @Mock private PagedResponse<PathItem> mockPagedResponse2;
  @Mock private PathItem mockPathItem1;
  @Mock private PathItem mockPathItem2;

  private AzureAsyncStorageClient azureAsyncStorageClient;
  private static final String AZURE_URI =
      "https://testaccount.dfs.core.windows.net/test-container/test-file";
  private static final String TEST_CONTAINER = "test-container";
  private static final String TEST_FILE = "test-file";

  @BeforeEach
  void setup() {
    lenient()
        .when(mockAzureStorageClientProvider.getAzureAsyncClient())
        .thenReturn(mockDataLakeServiceAsyncClient);
    lenient().when(mockStorageUtils.getBucketNameFromUri(AZURE_URI)).thenReturn(TEST_CONTAINER);
    lenient().when(mockStorageUtils.getPathFromUrl(AZURE_URI)).thenReturn(TEST_FILE);
    azureAsyncStorageClient =
        new AzureAsyncStorageClient(
            mockAzureStorageClientProvider, mockStorageUtils, ForkJoinPool.commonPool());
  }

  @Test
  void testListAllFilesInDir() throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String dirName = "dir1/";
    String continuationToken = "page_2";
    String prefix = TEST_FILE + "/";

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.listPaths(any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));
    when(mockPagedFlux.byPage(continuationToken)).thenReturn(Flux.just(mockPagedResponse2));

    // First page
    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockPathItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(continuationToken);
    when(mockPathItem1.getName()).thenReturn(prefix + fileName);
    when(mockPathItem1.isDirectory()).thenReturn(false);
    when(mockPathItem1.getLastModified()).thenReturn(OffsetDateTime.now());

    // Second page
    when(mockPagedResponse2.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockPathItem2)));
    when(mockPagedResponse2.getContinuationToken()).thenReturn(null);
    when(mockPathItem2.getName()).thenReturn(prefix + dirName);
    when(mockPathItem2.isDirectory()).thenReturn(true);

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

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.listPaths(any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage(continuationToken))
        .thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockPathItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(nextToken);
    when(mockPathItem1.getName()).thenReturn(prefix + "/" + fileName);
    when(mockPathItem1.isDirectory()).thenReturn(false);
    when(mockPathItem1.getLastModified()).thenReturn(OffsetDateTime.now());

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

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.listPaths(any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockPathItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(null);
    when(mockPathItem1.getName()).thenReturn(prefix + "/" + fileName);
    when(mockPathItem1.isDirectory()).thenReturn(false);
    when(mockPathItem1.getLastModified()).thenReturn(OffsetDateTime.now());

    Pair<String, List<File>> result =
        azureAsyncStorageClient.fetchObjectsByPage(TEST_CONTAINER, prefix, null, null).get();

    assertNull(result.getLeft());
    assertEquals(1, result.getRight().size());
  }

  @Test
  void testReadBlob() throws ExecutionException, InterruptedException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(fileContent);

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenReturn(Flux.just(byteBuffer));

    BinaryData result = azureAsyncStorageClient.readBlob(AZURE_URI).get();

    assertNotNull(result);
    assertArrayEquals(fileContent, result.toBytes());
  }

  @Test
  void testReadBlobMultipleChunks() throws ExecutionException, InterruptedException {
    // Azure SDK streams larger files as multiple ByteBuffer chunks. Previously the
    byte[] chunk1 = "first chunk ".getBytes(StandardCharsets.UTF_8);
    byte[] chunk2 = "second chunk ".getBytes(StandardCharsets.UTF_8);
    byte[] chunk3 = "third chunk".getBytes(StandardCharsets.UTF_8);
    byte[] expected = "first chunk second chunk third chunk".getBytes(StandardCharsets.UTF_8);

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read())
        .thenReturn(
            Flux.just(ByteBuffer.wrap(chunk1), ByteBuffer.wrap(chunk2), ByteBuffer.wrap(chunk3)));

    BinaryData result = azureAsyncStorageClient.readBlob(AZURE_URI).get();

    assertNotNull(result);
    assertArrayEquals(expected, result.toBytes());
  }

  @Test
  void testStreamFileAsync() throws ExecutionException, InterruptedException, IOException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(fileContent);

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenReturn(Flux.just(byteBuffer));

    FileStreamData result = azureAsyncStorageClient.streamFileAsync(AZURE_URI).get();

    assertNotNull(result);
    assertEquals(fileContent.length, result.getFileSize());

    byte[] resultContent = toByteArray(result.getInputStream());
    assertArrayEquals(fileContent, resultContent);
  }

  @Test
  void testReadFileAsBytes() throws ExecutionException, InterruptedException {
    byte[] fileContent = "test content".getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(fileContent);

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenReturn(Flux.just(byteBuffer));

    byte[] result = azureAsyncStorageClient.readFileAsBytes(AZURE_URI).get();

    assertArrayEquals(fileContent, result);
  }

  @ParameterizedTest
  @MethodSource("generateDataLakeStorageExceptionTestCases")
  void testReadBlobWithDataLakeStorageException(
      DataLakeStorageException exception, Class<? extends RuntimeException> expectedExceptionClass) {
    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenAnswer(invocation -> Flux.error(exception));

    CompletableFuture<BinaryData> future = azureAsyncStorageClient.readBlob(AZURE_URI);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(expectedExceptionClass, cause);
  }

  static Stream<Arguments> generateDataLakeStorageExceptionTestCases() {
    DataLakeStorageException accessDenied403 = mock(DataLakeStorageException.class);
    when(accessDenied403.getStatusCode()).thenReturn(403);
    when(accessDenied403.getMessage()).thenReturn("Access denied");
    when(accessDenied403.getErrorCode()).thenReturn(null);

    DataLakeStorageException unauthorized401 = mock(DataLakeStorageException.class);
    when(unauthorized401.getStatusCode()).thenReturn(401);
    when(unauthorized401.getMessage()).thenReturn("Unauthorized");
    when(unauthorized401.getErrorCode()).thenReturn(null);

    DataLakeStorageException pathNotFound = mock(DataLakeStorageException.class);
    when(pathNotFound.getStatusCode()).thenReturn(404);
    when(pathNotFound.getErrorCode()).thenReturn("PathNotFound");
    when(pathNotFound.getMessage()).thenReturn("Path not found");

    DataLakeStorageException filesystemNotFound = mock(DataLakeStorageException.class);
    when(filesystemNotFound.getStatusCode()).thenReturn(404);
    when(filesystemNotFound.getErrorCode()).thenReturn("FilesystemNotFound");
    when(filesystemNotFound.getMessage()).thenReturn("Filesystem not found");

    DataLakeStorageException tooManyRequests = mock(DataLakeStorageException.class);
    when(tooManyRequests.getStatusCode()).thenReturn(429);
    when(tooManyRequests.getMessage()).thenReturn("Too many requests");
    when(tooManyRequests.getErrorCode()).thenReturn(null);

    DataLakeStorageException serviceUnavailable = mock(DataLakeStorageException.class);
    when(serviceUnavailable.getStatusCode()).thenReturn(503);
    when(serviceUnavailable.getMessage()).thenReturn("Service unavailable");
    when(serviceUnavailable.getErrorCode()).thenReturn(null);

    DataLakeStorageException internalError = mock(DataLakeStorageException.class);
    when(internalError.getStatusCode()).thenReturn(500);
    when(internalError.getMessage()).thenReturn("Internal error");
    when(internalError.getErrorCode()).thenReturn(null);

    return Stream.of(
        Arguments.of(accessDenied403, AccessDeniedException.class),
        Arguments.of(unauthorized401, AccessDeniedException.class),
        Arguments.of(pathNotFound, NoSuchKeyException.class),
        Arguments.of(filesystemNotFound, NoSuchKeyException.class),
        Arguments.of(tooManyRequests, RateLimitException.class),
        Arguments.of(serviceUnavailable, RateLimitException.class),
        Arguments.of(internalError, ObjectStorageClientException.class));
  }

  @ParameterizedTest
  @MethodSource("generateWrappedExceptionTestCases")
  void testReadBlobWithWrappedException(
      RuntimeException exception, Class<? extends RuntimeException> expectedExceptionClass) {
    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenAnswer(invocation -> Flux.error(exception));

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
    DataLakeStorageException exception = mock(DataLakeStorageException.class);
    when(exception.getStatusCode()).thenReturn(429);
    when(exception.getMessage()).thenReturn("Throttled");
    when(exception.getErrorCode()).thenReturn(null);

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.listPaths(any()))
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

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.getFileAsyncClient(TEST_FILE)).thenReturn(mockFileAsyncClient);
    when(mockFileAsyncClient.read()).thenAnswer(invocation -> Flux.error(exception));

    CompletableFuture<BinaryData> future = azureAsyncStorageClient.readBlob(AZURE_URI);
    CompletionException executionException = assertThrows(CompletionException.class, future::join);

    Throwable cause = executionException.getCause();
    assertInstanceOf(ObjectStorageClientException.class, cause);
  }

  @Test
  void testFetchObjectsByPageWithDirectoryItems() throws ExecutionException, InterruptedException {
    String dirName = "dir1/";
    String prefix = "prefix";

    when(mockDataLakeServiceAsyncClient.getFileSystemAsyncClient(TEST_CONTAINER))
        .thenReturn(mockFileSystemAsyncClient);
    when(mockFileSystemAsyncClient.listPaths(any()))
        .thenReturn(mockPagedFlux);
    when(mockPagedFlux.byPage()).thenReturn(Flux.just(mockPagedResponse1));

    when(mockPagedResponse1.getElements())
        .thenReturn(IterableStream.of(Arrays.asList(mockPathItem1)));
    when(mockPagedResponse1.getContinuationToken()).thenReturn(null);
    when(mockPathItem1.getName()).thenReturn(prefix + dirName);
    when(mockPathItem1.isDirectory()).thenReturn(true);

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
