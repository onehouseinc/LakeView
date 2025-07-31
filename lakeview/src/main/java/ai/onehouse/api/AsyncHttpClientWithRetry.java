package ai.onehouse.api;

import static ai.onehouse.constants.ApiConstants.ACCEPTABLE_HTTP_FAILURE_STATUS_CODES;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

@Slf4j
public class AsyncHttpClientWithRetry {

  private final ScheduledExecutorService scheduler;
  private final int maxRetries;
  private final long retryDelayMillis;
  /** OkHttpClient that routes traffic through a configured proxy */
  private final OkHttpClient proxyEnabledClient;
  /** OkHttpClient that does not use any proxy (legacy behaviour) */
  private final OkHttpClient noProxyClient;
  private static final long MAX_RETRY_DELAY_MILLIS = 10000; // 10seconds
  private static final Random random = new Random();

  public AsyncHttpClientWithRetry(
      int maxRetries,
      long retryDelayMillis,
      @Nonnull OkHttpClient proxyEnabledClient,
      @Nonnull OkHttpClient noProxyClient) {
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.proxyEnabledClient = proxyEnabledClient;
    this.noProxyClient = noProxyClient;
  }

  /**
   * Backwards compatible constructor that takes a single OkHttpClient instance. The same instance
   * will be used for both proxy and non-proxy requests which preserves the behaviour that existed
   * before the dual-client support was introduced.
   */
  public AsyncHttpClientWithRetry(int maxRetries, long retryDelayMillis, OkHttpClient okHttpClient) {
    this(maxRetries, retryDelayMillis, okHttpClient, okHttpClient);
  }

  /**
   * Makes an asynchronous HTTP request with retries.
   *
   * @param request   The OkHttp {@link Request} to execute.
   * @param useProxy  When {@code true} the request will be executed using the proxy-enabled client,
   *                  otherwise the legacy no-proxy client will be used.
   * @return {@link CompletableFuture} wrapping the {@link Response}.
   */
  public CompletableFuture<Response> makeRequestWithRetry(Request request, boolean useProxy) {
    OkHttpClient client = useProxy ? proxyEnabledClient : noProxyClient;
    return attemptRequest(client, request, 1);
  }

  // Maintains compatibility with existing call-sites.
  public CompletableFuture<Response> makeRequestWithRetry(Request request) {
    return makeRequestWithRetry(request, true);
  }

  private CompletableFuture<Response> attemptRequest(OkHttpClient client, Request request, int tryCount) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    client
        .newCall(request)
        .enqueue(
            new Callback() {
              @Override
              public void onFailure(@Nonnull Call call, @Nonnull IOException e) {
                if (tryCount < maxRetries) {
                  Request request = call.request();
                  HttpUrl url = request.url();
                  String method = request.method();
                  log.warn(
                      "API Request failed with error: {}, attempt: {}, url: {}, method: {}",
                      e.getMessage(),
                      tryCount,
                      url,
                      method);

                  scheduleRetry(client, request, tryCount, future);
                } else {
                  future.completeExceptionally(e);
                }
              }

              @Override
              public void onResponse(@Nonnull Call call, @Nonnull Response response) {
                if (!response.isSuccessful()
                    && !ACCEPTABLE_HTTP_FAILURE_STATUS_CODES.contains(response.code())
                    && tryCount < maxRetries) {
                  Request request = call.request();
                  HttpUrl url = request.url();
                  String method = request.method();
                  int statusCode = response.code();
                  log.warn(
                      "API Request failed with HTTP status: {}, attempt: {}, url: {}, method: {}",
                      statusCode,
                      tryCount,
                      url,
                      method);
                  response.close();
                  scheduleRetry(client, request, tryCount, future);
                } else {
                  future.complete(response);
                }
              }
            });

    return future;
  }

  private void scheduleRetry(
      OkHttpClient client, Request request, int tryCount, CompletableFuture<Response> future) {
    scheduler.schedule(
        () -> {
          log.info("Scheduling request with attempt: {}", (tryCount + 1));
          attemptRequest(client, request, tryCount + 1)
              .whenComplete(
                  (resp, throwable) -> {
                    if (throwable != null) {
                      future.completeExceptionally(throwable);
                    } else {
                      future.complete(resp);
                    }
                  });
        },
        calculateDelay(tryCount),
        TimeUnit.MILLISECONDS);
  }

  private long calculateDelay(int tryCount) {
    // Exponential backoff with jitter and upper bound
    long delay = (long) (retryDelayMillis * Math.pow(2, tryCount));
    long jitter = (long) (random.nextDouble() * delay) - (delay / 2);
    return Math.min(delay + jitter, MAX_RETRY_DELAY_MILLIS);
  }

  public void shutdownScheduler() {
    scheduler.shutdown();
    proxyEnabledClient.connectionPool().evictAll();
    proxyEnabledClient.dispatcher().executorService().shutdown();
    if (noProxyClient != proxyEnabledClient) {
      noProxyClient.connectionPool().evictAll();
      noProxyClient.dispatcher().executorService().shutdown();
    }
  }

  @VisibleForTesting
  public long getRetryDelayMillis() {
    return retryDelayMillis;
  }

  @VisibleForTesting
  public int getMaxRetries() {
    return maxRetries;
  }
}
