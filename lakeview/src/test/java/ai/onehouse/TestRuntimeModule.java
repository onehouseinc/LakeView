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
import ai.onehouse.metadata_extractor.HoodiePropertiesReader;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import ai.onehouse.metadata_extractor.TableDiscoveryService;
import ai.onehouse.metadata_extractor.TimelineCommitInstantsUploader;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.GCSAsyncStorageClient;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.providers.GcsClientProvider;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.util.Modules;
import java.util.concurrent.ExecutorService;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import ai.onehouse.metrics.Metrics;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.api.OnehouseApiClient;

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

  static class GuiceTestModule extends AbstractModule {
    private final Config config;
    private final StorageUtils storageUtils;
    private final S3AsyncClientProvider s3Provider;
    private final GcsClientProvider gcsProvider;
    private final ExecutorService executorService;
    private final Metrics metrics;
    private final LakeViewExtractorMetrics lakeViewExtractorMetrics;
    private final AsyncHttpClientWithRetry httpClient;
    private final OnehouseApiClient onehouseApiClient;

    GuiceTestModule(Config config, StorageUtils storageUtils, S3AsyncClientProvider s3Provider,
                    GcsClientProvider gcsProvider, ExecutorService executorService,
                    Metrics metrics, LakeViewExtractorMetrics lakeViewExtractorMetrics,
                    AsyncHttpClientWithRetry httpClient, OnehouseApiClient onehouseApiClient) {
      this.config = config;
      this.storageUtils = storageUtils;
      this.s3Provider = s3Provider;
      this.gcsProvider = gcsProvider;
      this.executorService = executorService;
      this.metrics = metrics;
      this.lakeViewExtractorMetrics = lakeViewExtractorMetrics;
      this.httpClient = httpClient;
      this.onehouseApiClient = onehouseApiClient;
    }

    @Override
    protected void configure() {
      bind(Config.class).toInstance(config);
      bind(AsyncHttpClientWithRetry.class).toInstance(httpClient);
      bind(StorageUtils.class).toInstance(storageUtils);
      bind(S3AsyncClientProvider.class).toInstance(s3Provider);
      bind(GcsClientProvider.class).toInstance(gcsProvider);
      bind(ExecutorService.class).toInstance(executorService);
      bind(Metrics.class).toInstance(metrics);
      bind(LakeViewExtractorMetrics.class).toInstance(lakeViewExtractorMetrics);
      bind(OnehouseApiClient.class).toInstance(onehouseApiClient);
    }
  }

  @Test
  void testGuiceBootstrapping_S3_and_GCS() {
    // S3 setup
    FileSystemConfiguration mockFsConfig = mock(FileSystemConfiguration.class);
    S3Config mockS3Config = mock(S3Config.class);
    when(mockConfig.getFileSystemConfiguration()).thenReturn(mockFsConfig);
    when(mockFsConfig.getS3Config()).thenReturn(mockS3Config);

    Injector injectorS3 = Guice.createInjector(
        Modules.override(new RuntimeModule(mockConfig))
            .with(new GuiceTestModule(
                mockConfig,
                mock(StorageUtils.class),
                mock(S3AsyncClientProvider.class),
                mock(GcsClientProvider.class),
                mock(ExecutorService.class),
                mock(Metrics.class),
                mock(LakeViewExtractorMetrics.class),
                mock(AsyncHttpClientWithRetry.class),
                mock(OnehouseApiClient.class)))
    );

    AsyncStorageClient s3ClientUpload = injectorS3.getInstance(
        Key.get(AsyncStorageClient.class, RuntimeModule.TableMetadataUploadObjectStorageAsyncClient.class));
    Assertions.assertInstanceOf(S3AsyncStorageClient.class, s3ClientUpload);
    AsyncStorageClient s3ClientDiscovery = injectorS3.getInstance(
        Key.get(AsyncStorageClient.class, RuntimeModule.TableDiscoveryObjectStorageAsyncClient.class));
    Assertions.assertInstanceOf(S3AsyncStorageClient.class, s3ClientDiscovery);


    // GCS setup
    FileSystemConfiguration mockFsConfigGcs = mock(FileSystemConfiguration.class);
    GCSConfig mockGcsConfig = mock(GCSConfig.class);
    when(mockConfig.getFileSystemConfiguration()).thenReturn(mockFsConfigGcs);
    when(mockFsConfigGcs.getGcsConfig()).thenReturn(mockGcsConfig);

    Metrics mockMetrics = mock(Metrics.class);
    LakeViewExtractorMetrics mockLakeViewExtractorMetrics = mock(LakeViewExtractorMetrics.class);
    AsyncHttpClientWithRetry mockHttpClient = mock(AsyncHttpClientWithRetry.class);
    OnehouseApiClient mockOnehouseApiClient = mock(OnehouseApiClient.class);

    Injector injectorGcs = Guice.createInjector(
        Modules.override(new RuntimeModule(mockConfig))
            .with(new GuiceTestModule(
                mockConfig, mock(StorageUtils.class), mock(S3AsyncClientProvider.class),
                mock(GcsClientProvider.class), mock(ExecutorService.class),
                mockMetrics, mockLakeViewExtractorMetrics,
                mockHttpClient, mockOnehouseApiClient))
    );

    AsyncStorageClient gcsClientUpload = injectorGcs.getInstance(
        Key.get(AsyncStorageClient.class, RuntimeModule.TableMetadataUploadObjectStorageAsyncClient.class));
    Assertions.assertInstanceOf(GCSAsyncStorageClient.class, gcsClientUpload);
    AsyncStorageClient gcsClientDiscovery = injectorGcs.getInstance(
        Key.get(AsyncStorageClient.class, RuntimeModule.TableDiscoveryObjectStorageAsyncClient.class));
    Assertions.assertInstanceOf(GCSAsyncStorageClient.class, gcsClientDiscovery);

    // Common assertions
    Assertions.assertNotNull(injectorS3.getInstance(Config.class));
    Assertions.assertNotNull(injectorS3.getInstance(StorageUtils.class));
    Assertions.assertNotNull(injectorS3.getInstance(S3AsyncClientProvider.class));
    Assertions.assertNotNull(injectorS3.getInstance(ExecutorService.class));
    Assertions.assertNotNull(injectorS3.getInstance(HoodiePropertiesReader.class));
    Assertions.assertNotNull(injectorS3.getInstance(TableDiscoveryAndUploadJob.class));
    Assertions.assertNotNull(injectorS3.getInstance(TableDiscoveryService.class));
    Assertions.assertNotNull(injectorS3.getInstance(TimelineCommitInstantsUploader.class));
    Assertions.assertNotNull(injectorGcs.getInstance(GcsClientProvider.class));
    Assertions.assertNotNull(injectorGcs.getInstance(HoodiePropertiesReader.class));
    Assertions.assertNotNull(injectorGcs.getInstance(TableDiscoveryAndUploadJob.class));
    Assertions.assertNotNull(injectorGcs.getInstance(TableDiscoveryService.class));
    Assertions.assertNotNull(injectorGcs.getInstance(TimelineCommitInstantsUploader.class));
  }

  enum FileSystem {
    S3,
    GCS
  }
}
