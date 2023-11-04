package com.onehouse.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.onehouse.storage.models.File;
import com.onehouse.storage.providers.GcsClientProvider;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GCSAsyncStorageClientTest {

  @Mock private GcsClientProvider mockGcsClientProvider;
  @Mock private StorageUtils mockStorageUtils;
  @Mock private Storage mockGcsClient;
  @Mock private Bucket mockBucket;
  @Mock private Blob mockBlob1;
  @Mock private Blob mockBlob2;
  @Mock private Page<Blob> mockPage;
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
  public void testListAllFilesInDir() throws ExecutionException, InterruptedException {
    String fileName = "file1";
    String dirName = "dir1/";

    when(mockGcsClient.get(TEST_BUCKET)).thenReturn(mockBucket);
    when(mockBucket.list(
            Storage.BlobListOption.prefix(TEST_KEY + "/"), Storage.BlobListOption.delimiter("/")))
        .thenReturn(mockPage);
    when(mockPage.iterateAll()).thenReturn(ImmutableList.of(mockBlob1, mockBlob2));
    when(mockBlob1.getName()).thenReturn(TEST_KEY + "/" + fileName);
    when(mockBlob2.getName()).thenReturn(TEST_KEY + "/" + dirName);
    when(mockBlob1.isDirectory()).thenReturn(false);
    when(mockBlob2.isDirectory()).thenReturn(true);
    when(mockBlob1.getUpdateTime()).thenReturn(0L);

    List<File> result = gcsAsyncStorageClient.listAllFilesInDir(GCS_URI).get();

    List<File> expectedFiles =
        List.of(
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
  }

  @Test
  public void testReadBlob() throws ExecutionException, InterruptedException, IOException {
    when(mockGcsClient.get(BlobId.of(TEST_BUCKET, TEST_KEY))).thenReturn(mockBlob1);

    Blob blob = gcsAsyncStorageClient.readBlob(GCS_URI).get();
    assertNotNull(blob);
  }
}