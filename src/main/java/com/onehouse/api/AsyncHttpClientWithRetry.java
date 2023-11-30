package com.onehouse.api;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  private final OkHttpClient okHttpClient;
  private static final long MAX_RETRY_DELAY_MILLIS = 10000; // 10seconds
  // using mapping from:
  // https://chromium.googlesource.com/external/github.com/grpc/grpc/+/refs/tags/v1.21.4-pre1/doc/statuscodes.md
  private static final List<Integer> ACCEPTABLE_HTTP_FAILURE_STATUS_CODES =
          new ArrayList<>(Arrays.asList(404, 400, 403, 401, 409));

  private static final Random random = new Random();

  public AsyncHttpClientWithRetry(
      int maxRetries, long retryDelayMillis, OkHttpClient okHttpClient) {
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.okHttpClient = okHttpClient;
  }

  public CompletableFuture<Response> makeRequestWithRetry(Request request) {
    return attemptRequest(request, 1);
  }

  private CompletableFuture<Response> attemptRequest(Request request, int tryCount) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    okHttpClient
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

                  scheduleRetry(request, tryCount, future);
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
                  scheduleRetry(request, tryCount, future);
                } else {
                  future.complete(response);
                }
              }
            });

    return future;
  }

  private void scheduleRetry(Request request, int tryCount, CompletableFuture<Response> future) {
    scheduler.schedule(
        () -> {
          log.info("Scheduling request with attempt: {}", (tryCount + 1));
          attemptRequest(request, tryCount + 1)
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
