package com.onehouse;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.config.Config;
import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.GCSConfig;
import com.onehouse.config.models.common.S3Config;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.GCSAsyncStorageClient;
import com.onehouse.storage.S3AsyncStorageClient;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.providers.GcsClientProvider;
import com.onehouse.storage.providers.S3AsyncClientProvider;
import java.util.concurrent.ExecutorService;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

class TestRuntimeModule {
  private RuntimeModule runtimeModule;
  private Config mockConfig;

  @BeforeEach
  void setUp() {
    mockConfig = Mockito.mock(Config.class);
    runtimeModule = new RuntimeModule(mockConfig);
  }

  @Test
  void testProvidesOkHttpClient() {
    ExecutorService mockExecutorService = Mockito.mock(ExecutorService.class);
    OkHttpClient okHttpClient = RuntimeModule.providesOkHttpClient(mockExecutorService);
    assertNotNull(okHttpClient);
  }

  @ParameterizedTest
  @EnumSource(FileSystem.class)
  void testProvidesAsyncStorageClient(FileSystem fileSystemType) {
    StorageUtils mockStorageUtils = mock(StorageUtils.class);
    ExecutorService mockExecutorService = mock(ExecutorService.class);
    FileSystemConfiguration mockFileSystemConfiguration = mock(FileSystemConfiguration.class);
    S3AsyncClientProvider mockS3AsyncClientProvider = mock(S3AsyncClientProvider.class);
    GcsClientProvider mockGcsClientProvider = mock(GcsClientProvider.class);

    when(mockConfig.getFileSystemConfiguration()).thenReturn(mockFileSystemConfiguration);

    if (FileSystem.S3.equals(fileSystemType)) {
      S3Config mockS3Config = mock(S3Config.class);
      when(mockFileSystemConfiguration.getS3Config()).thenReturn(mockS3Config);
      when(mockS3AsyncClientProvider.getS3AsyncClient()).thenReturn(null);
    } else {
      GCSConfig mockGcsConfig = mock(GCSConfig.class);
      when(mockFileSystemConfiguration.getGcsConfig()).thenReturn(mockGcsConfig);
      when(mockGcsClientProvider.getGcsClient()).thenReturn(null);
    }

    AsyncStorageClient asyncStorageClient =
        RuntimeModule.providesAsyncStorageClient(
            mockConfig,
            mockStorageUtils,
            mockS3AsyncClientProvider,
            mockGcsClientProvider,
            mockExecutorService);
    if (FileSystem.S3.equals(fileSystemType)) {
      assertTrue(asyncStorageClient instanceof S3AsyncStorageClient);
    } else {
      assertTrue(asyncStorageClient instanceof GCSAsyncStorageClient);
    }
  }

  @Test
  void testProvidesHttpAsyncClient() {
    OkHttpClient mockOkHttpClient = mock(OkHttpClient.class);
    AsyncHttpClientWithRetry asyncHttpClientWithRetry =
        runtimeModule.providesHttpAsyncClient(mockOkHttpClient);
    assertEquals(runtimeModule.getHttpClientMaxRetries(), asyncHttpClientWithRetry.getMaxRetries());
    assertEquals(
        runtimeModule.getHttpClientRetryDelayMs(), asyncHttpClientWithRetry.getRetryDelayMillis());
  }

  enum FileSystem {
    S3,
    GCS
  }
}
