package ai.onehouse;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.GCSConfig;
import ai.onehouse.config.models.common.S3Config;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.GCSAsyncStorageClient;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.providers.GcsClientProvider;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import java.util.concurrent.ExecutorService;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Assertions;
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

    AsyncStorageClient asyncStorageClientForDiscovery =
        RuntimeModule.providesAsyncStorageClientForDiscovery(
            mockConfig,
            mockStorageUtils,
            mockS3AsyncClientProvider,
            mockGcsClientProvider,
            mockExecutorService);
    if (FileSystem.S3.equals(fileSystemType)) {
      assertTrue(asyncStorageClientForDiscovery instanceof S3AsyncStorageClient);
    } else {
      assertTrue(asyncStorageClientForDiscovery instanceof GCSAsyncStorageClient);
    }

    AsyncStorageClient asyncStorageClientForUpload =
        RuntimeModule.providesAsyncStorageClientForUpload(
            mockConfig,
            mockStorageUtils,
            mockS3AsyncClientProvider,
            mockGcsClientProvider,
            mockExecutorService);
    if (FileSystem.S3.equals(fileSystemType)) {
      Assertions.assertInstanceOf(S3AsyncStorageClient.class, asyncStorageClientForUpload);
    } else {
      Assertions.assertInstanceOf(GCSAsyncStorageClient.class, asyncStorageClientForUpload);
    }

    S3AsyncClientProvider s3AsyncClientProviderForDiscovery =
        RuntimeModule.providesS3AsyncClientProviderForDiscovery(mockConfig, mockExecutorService);
    Assertions.assertInstanceOf(S3AsyncClientProvider.class, s3AsyncClientProviderForDiscovery);

    S3AsyncClientProvider s3AsyncClientProviderForUpload =
        RuntimeModule.providesS3AsyncClientProviderForUpload(mockConfig, mockExecutorService);
    Assertions.assertInstanceOf(S3AsyncClientProvider.class, s3AsyncClientProviderForUpload);
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
