package ai.onehouse.storage;

import static ai.onehouse.constants.MetricsConstants.MetadataUploadFailureReasons.ACCESS_DENIED;
import static ai.onehouse.constants.MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.AccessDeniedException;
import ai.onehouse.exceptions.ObjectStorageClientException;
import ai.onehouse.exceptions.RateLimitException;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import ai.onehouse.storage.providers.GcsClientProvider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GCSAsyncStorageClientTest {

  @Mock private GcsClientProvider mockGcsClientProvider;
  @Mock private StorageUtils mockStorageUtils;
  @Mock private Storage mockGcsClient;
  @Mock private Blob mockBlob1;
  @Mock private Blob mockBlob2;
  @Mock private Page<Blob> mockPage1;
  @Mock private Page<Blob> mockPage2;
  private GCSAsyncStorageClient gcsAsyncStorageClient;
  private static final String GCS_URI = "gs://test-bucket/test-key";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_KEY = "test-key";

  @BeforeEach
  void setup() {
    when(mockGcsClientProvider.getGcsClient()).thenReturn(mockGcsClient);
    when(mockStorageUtils.getBucketNameFromUri(GCS_URI)).thenReturn(TEST_BUCKET);
    when(mockStorageUtils.getPathFromUrl(GCS_URI)).thenReturn(TEST_KEY);
    gcsAsyncStorageClient =
        new GCSAsyncStorageClient(
            mockGcsClientProvider, mockStorageUtils, ForkJoinPool.commonPool());
  }

  @Test
  void testListAllFilesInDir() {
    String fileName = "file1";
    String dirName = "dir1/";
    String pageToken = "page_2";

    when(mockGcsClient.list(
            TEST_BUCKET,
            Storage.BlobListOption.prefix(TEST_KEY + "/"),
            Storage.BlobListOption.delimiter("/")))
        .thenReturn(mockPage1)
      .thenThrow(new RuntimeException("error-message"));
    when(mockGcsClient.list(
            TEST_BUCKET,
            Storage.BlobListOption.prefix(TEST_KEY + "/"),
            Storage.BlobListOption.delimiter("/"),
            Storage.BlobListOption.pageToken(pageToken)))
        .thenReturn(mockPage2);
    when(mockPage1.getValues()).thenReturn(ImmutableList.of(mockBlob1));
    when(mockPage1.hasNextPage()).thenReturn(true);
    when(mockPage1.getNextPageToken()).thenReturn(pageToken);
    when(mockPage2.getValues()).thenReturn(ImmutableList.of(mockBlob2));
    when(mockPage2.hasNextPage()).thenReturn(false);
    when(mockBlob1.getName()).thenReturn(TEST_KEY + "/" + fileName);
    when(mockBlob2.getName()).thenReturn(TEST_KEY + "/" + dirName);
    when(mockBlob1.isDirectory()).thenReturn(false);
    when(mockBlob2.isDirectory()).thenReturn(true);
    when(mockBlob1.getUpdateTime()).thenReturn(0L);

    List<File> result = gcsAsyncStorageClient.listAllFilesInDir(GCS_URI).join();

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

    assertEquals(expectedFiles, result);
    CompletionException exception = assertThrows(CompletionException.class, gcsAsyncStorageClient.listAllFilesInDir(GCS_URI)::join);
    assertInstanceOf(ObjectStorageClientException.class, exception.getCause());
  }

  @Test
  void testReadBlob() {
    when(mockGcsClient.get(BlobId.of(TEST_BUCKET, TEST_KEY)))
      .thenReturn(mockBlob1)
      .thenThrow(new RuntimeException("some-error"));

    Blob blob = gcsAsyncStorageClient.readBlob(GCS_URI).join();
    assertNotNull(blob);

    CompletionException exception = assertThrows(CompletionException.class, gcsAsyncStorageClient.readBlob(GCS_URI)::join);
    assertInstanceOf(ObjectStorageClientException.class, exception.getCause());
  }

  @ParameterizedTest
  @MethodSource("generateTestCases")
  void testReadBlobExceptions(RuntimeException exceptionToThrow, Throwable throwable) {
    when(mockGcsClient.get(BlobId.of(TEST_BUCKET, TEST_KEY)))
        .thenThrow(exceptionToThrow);

    CompletionException exception = assertThrows(CompletionException.class, gcsAsyncStorageClient.readBlob(GCS_URI)::join);
    assertInstanceOf(throwable.getClass(), exception.getCause());
  }

  static Stream<Arguments> generateTestCases() {
    return Stream.of(
        Arguments.of(
            new StorageException(403, "List permission missing"),
            new AccessDeniedException("error")),
        Arguments.of(new StorageException(401, "Unauthorized"),
            new AccessDeniedException("error")),
        Arguments.of(new StorageException(0, "Error requesting access token"),
            new AccessDeniedException("error")),
        Arguments.of(new AccessDeniedException("error"),
            new AccessDeniedException("error")),
        Arguments.of(new StorageException(500, "Internal"),
            new ObjectStorageClientException("error")));
  }

  @Test
  void testStreamFileAsync() throws ExecutionException, InterruptedException, IOException {
    long fileSize = 1024L;
    byte[] fileContent = "test content".getBytes();
    ReadChannel mockReadChannel = mock(ReadChannel.class);

    when(mockGcsClient.get(BlobId.of(TEST_BUCKET, TEST_KEY))).thenReturn(mockBlob1);
    when(mockBlob1.getSize()).thenReturn(fileSize);
    when(mockBlob1.reader()).thenReturn(mockReadChannel);

    // Set up the mock ReadChannel to return our test content
    ByteBuffer buffer = ByteBuffer.wrap(fileContent);
    when(mockReadChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer arg = invocation.getArgument(0);
              int remaining = Math.min(arg.remaining(), buffer.remaining());
              byte[] data = new byte[remaining];
              buffer.get(data);
              arg.put(data);
              return remaining > 0 ? remaining : -1;
            });

    CompletableFuture<FileStreamData> future = gcsAsyncStorageClient.streamFileAsync(GCS_URI);
    FileStreamData result = future.get();

    assertNotNull(result);
    assertEquals(fileSize, result.getFileSize());

    // Read the content from the InputStream
    byte[] resultContent = toByteArray(result.getInputStream());
    assertArrayEquals(fileContent, resultContent);
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
