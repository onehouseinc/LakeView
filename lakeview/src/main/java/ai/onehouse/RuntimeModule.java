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
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
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

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface ProxyEnabledHttpClient {}

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface NoProxyHttpClient {}

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
  @ProxyEnabledHttpClient
  static OkHttpClient providesOkHttpClientWithProxy(ExecutorService executorService) {
    Dispatcher dispatcher = new Dispatcher(executorService);
    OkHttpClient.Builder builder =
        new OkHttpClient.Builder()
            .readTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .writeTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .connectTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .dispatcher(dispatcher);

    String httpProxyEnv = System.getenv("HTTP_PROXY");
    if (httpProxyEnv != null && !httpProxyEnv.trim().isEmpty()) {
      Proxy proxy = buildProxy(httpProxyEnv);
      String noProxyEnv = System.getenv("NO_PROXY");
      if (noProxyEnv != null && !noProxyEnv.trim().isEmpty()) {
        builder.proxySelector(new EnvProxySelector(proxy, noProxyEnv));
      } else {
        builder.proxy(proxy);
      }
      log.info("Configured OkHttp client to use proxy from HTTP_PROXY env var: {}", httpProxyEnv);
    }

    return builder.build();
  }

  // Alias retained for unit tests that directly invoke this helper.
  // Note: This method is intentionally not annotated with @Provides to avoid duplicate bindings.
  static OkHttpClient providesOkHttpClient(ExecutorService executorService) {
    return providesOkHttpClientWithProxy(executorService);
  }

  @Provides
  @Singleton
  @NoProxyHttpClient
  static OkHttpClient providesOkHttpClientWithoutProxy(ExecutorService executorService) {
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
  static AsyncHttpClientWithRetry providesHttpAsyncClient(
      @ProxyEnabledHttpClient OkHttpClient proxyEnabledClient,
      @NoProxyHttpClient OkHttpClient noProxyClient) {
    return new AsyncHttpClientWithRetry(
        HTTP_CLIENT_MAX_RETRIES, HTTP_CLIENT_RETRY_DELAY_MS, proxyEnabledClient, noProxyClient);
  }

  // Backwards compatibility util for tests (not used by Guice)
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

  /** Builds a java.net.Proxy object from the HTTP_PROXY environment variable */
  private static Proxy buildProxy(String proxyEnv) {
    try {
      String proxyUrl = proxyEnv.matches("^[a-zA-Z]+://.*") ? proxyEnv : "http://" + proxyEnv;
      URI uri = URI.create(proxyUrl);
      String host = uri.getHost();
      int port = uri.getPort() == -1 ? 80 : uri.getPort();
      return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
    } catch (Exception e) {
      logger.error("Failed to parse proxy url: {}", proxyEnv, e);
      return Proxy.NO_PROXY;
    }
  }

  /** ProxySelector that respects the NO_PROXY environment variable */
  private static class EnvProxySelector extends ProxySelector {
    private final Proxy proxy;
    private final List<String> noProxyHosts;

    EnvProxySelector(Proxy proxy, String noProxyEnv) {
      this.proxy = proxy;
      this.noProxyHosts = parseNoProxy(noProxyEnv);
    }

    private static List<String> parseNoProxy(String noProxyEnv) {
      if (noProxyEnv == null || noProxyEnv.trim().isEmpty()) {
        return Collections.emptyList();
      }
      String[] parts = noProxyEnv.split(",");
      List<String> list = new ArrayList<>();
      for (String part : parts) {
        list.add(part.trim());
      }
      return list;
    }

    @Override
    public List<Proxy> select(URI uri) {
      String host = uri.getHost();
      if (host != null) {
        for (String pattern : noProxyHosts) {
          if (pattern.isEmpty()) {
            continue;
          }
          if (host.equals(pattern) || host.endsWith(pattern)) {
            return Collections.singletonList(Proxy.NO_PROXY);
          }
        }
      }
      return Collections.singletonList(proxy);
    }

    @Override
    public void connectFailed(URI uri, java.net.SocketAddress sa, java.io.IOException ioe) {
      logger.error("Proxy connection failed to {} via {}", uri, sa, ioe);
    }
  }
}
