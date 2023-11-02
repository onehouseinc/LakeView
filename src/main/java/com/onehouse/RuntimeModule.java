package com.onehouse;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.onehouse.config.Config;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.configV1.ConfigV1;
import com.onehouse.storage.AsyncStorageLister;
import com.onehouse.storage.AsyncStorageReader;
import com.onehouse.storage.GCSAsyncStorageLister;
import com.onehouse.storage.GCSAsyncStorageReader;
import com.onehouse.storage.S3AsyncStorageLister;
import com.onehouse.storage.S3AsyncStorageReader;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.providers.ClientProviderFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeModule.class);
  private static final int IO_WORKLOAD_NUM_THREAD_MULTIPLIER = 5;
  private static final int HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS = 5;
  private final Config config;

  public RuntimeModule(Config config) {
    this.config = config;
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
  static AsyncStorageLister providesAsyncStorageLister(
      Config config,
      ClientProviderFactory clientProviderFactory,
      StorageUtils storageUtils,
      ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    if (fileSystemConfiguration.getS3Config() != null) {
      return new S3AsyncStorageLister(
          clientProviderFactory.getS3AsyncClientProvider(), storageUtils);
    } else if (fileSystemConfiguration.getGcsConfig() != null) {
      return new GCSAsyncStorageLister(
          clientProviderFactory.getGcsClientProvider(), storageUtils, executorService);
    }
    throw new IllegalArgumentException(
        "Config should have either one of S3/Gcs filesystem configs");
  }

  @Provides
  @Singleton
  static AsyncStorageReader providesAsyncStorageReader(
      Config config,
      ClientProviderFactory clientProviderFactory,
      StorageUtils storageUtils,
      ExecutorService executorService) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    if (fileSystemConfiguration.getS3Config() != null) {
      return new S3AsyncStorageReader(
          clientProviderFactory.getS3AsyncClientProvider(), storageUtils);
    } else if (fileSystemConfiguration.getGcsConfig() != null) {
      return new GCSAsyncStorageReader(
          clientProviderFactory.getGcsClientProvider(), storageUtils, executorService);
    }
    throw new IllegalArgumentException(
        "Config should have either one of S3/Gcs filesystem configs");
  }

  @Provides
  @Singleton
  static ExecutorService providesExecutorService() {
    class ApplicationThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
      private static final String THREAD_GROUP_NAME_TEMPLATE = "metadata-extractor-io-%d";
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
        Runtime.getRuntime().availableProcessors()
            * IO_WORKLOAD_NUM_THREAD_MULTIPLIER, // more threads as most operation are IO intensive
        // workload
        new ApplicationThreadFactory(),
        (thread, throwable) -> {
          logger.error(
              String.format("Uncaught exception in a thread (%s)", thread.getName()), throwable);
        },
        // NOTE: It's squarely important to make sure
        // that `asyncMode` is true in async applications
        true);
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(config);
  }
}
