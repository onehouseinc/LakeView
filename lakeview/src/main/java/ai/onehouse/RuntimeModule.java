package ai.onehouse;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.GCSAsyncStorageClient;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.providers.GcsClientProvider;
import ai.onehouse.storage.providers.S3AsyncClientProvider;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class RuntimeModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeModule.class);
  private static final int IO_WORKLOAD_NUM_THREAD_MULTIPLIER = 5;
  private static final int HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS = 15;
  private static final int HTTP_CLIENT_MAX_RETRIES = 3;
  private static final long HTTP_CLIENT_RETRY_DELAY_MS = 1000;
  private final Config config;

  public RuntimeModule(Config config) {
    this.config = config;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface TableDiscoveryS3ObjectStorageClient {}

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface TableMetadataUploadS3ObjectStorageClient {}

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface TableDiscoveryObjectStorageAsyncClient {}

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface TableMetadataUploadObjectStorageAsyncClient {}

  @Provides
  @Singleton
  @TableDiscoveryS3ObjectStorageClient
  static S3AsyncClientProvider providesS3AsyncClientProviderForDiscovery(Config config, ExecutorService executorService) {
    return new S3AsyncClientProvider(config, executorService);
  }

  @Provides
  @Singleton
  @TableMetadataUploadS3ObjectStorageClient
  static S3AsyncClientProvider providesS3AsyncClientProviderForUpload(Config config, ExecutorService executorService) {
    return new S3AsyncClientProvider(config, executorService);
  }

  @Provides
  @Singleton
  static OkHttpClient providesOkHttpClient(ExecutorService executorService) {
    Dispatcher dispatcher = new Dispatcher(executorService);
    return new OkHttpClient.Builder()
        .readTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .writeTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .connectTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .dispatcher(dispatcher)
        .build();
  }

  @Provides
  @Singleton
  static AsyncHttpClientWithRetry providesHttpAsyncClient(OkHttpClient okHttpClient) {
    return new AsyncHttpClientWithRetry(
        HTTP_CLIENT_MAX_RETRIES, HTTP_CLIENT_RETRY_DELAY_MS, okHttpClient);
  }

  @Provides
  @Singleton
  @TableDiscoveryObjectStorageAsyncClient
  static AsyncStorageClient providesAsyncStorageClientForDiscovery(
      Config config,
      StorageUtils storageUtils,
      @TableDiscoveryS3ObjectStorageClient S3AsyncClientProvider s3AsyncClientProvider,
      GcsClientProvider gcsClientProvider,
      ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    if (fileSystemConfiguration.getS3Config() != null) {
      return new S3AsyncStorageClient(s3AsyncClientProvider, storageUtils, executorService);
    } else {
      return new GCSAsyncStorageClient(gcsClientProvider, storageUtils, executorService);
    }
  }

  @Provides
  @Singleton
  @TableMetadataUploadObjectStorageAsyncClient
  static AsyncStorageClient providesAsyncStorageClientForUpload(
      Config config,
      StorageUtils storageUtils,
      @TableMetadataUploadS3ObjectStorageClient S3AsyncClientProvider s3AsyncClientProvider,
      GcsClientProvider gcsClientProvider,
      ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    if (fileSystemConfiguration.getS3Config() != null) {
      return new S3AsyncStorageClient(s3AsyncClientProvider, storageUtils, executorService);
    } else {
      return new GCSAsyncStorageClient(gcsClientProvider, storageUtils, executorService);
    }
  }

  @Provides
  @Singleton
  static ConfigProvider configProvider(Config config) {
    return new ConfigProvider(config);
  }

  @Provides
  @Singleton
  static ExecutorService providesExecutorService() {
    // more threads as most operation are IO intensive workload
    int numThreads = Runtime.getRuntime().availableProcessors() * IO_WORKLOAD_NUM_THREAD_MULTIPLIER;
    log.info("Spinning up {} threads", numThreads);
    class ApplicationThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
      private static final String THREAD_GROUP_NAME_TEMPLATE = "metadata-extractor-%d";
      private final AtomicInteger counter = new AtomicInteger(1);

      @Override
      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new ForkJoinWorkerThread(pool) {
          {
            setName(String.format(THREAD_GROUP_NAME_TEMPLATE, counter.getAndIncrement()));
          }
        };
      }
    }

    return new ForkJoinPool(
        numThreads,
        new ApplicationThreadFactory(),
        (thread, throwable) -> {
          if (throwable != null) {
            logger.error(
                String.format("Uncaught exception in a thread (%s)", thread.getName()), throwable);
          }
        },
        // NOTE: It's squarely important to make sure
        // that `asyncMode` is true in async applications
        true);
  }

  @VisibleForTesting
  long getHttpClientRetryDelayMs() {
    return HTTP_CLIENT_RETRY_DELAY_MS;
  }

  @VisibleForTesting
  int getHttpClientMaxRetries() {
    return HTTP_CLIENT_MAX_RETRIES;
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(config);
  }
}
