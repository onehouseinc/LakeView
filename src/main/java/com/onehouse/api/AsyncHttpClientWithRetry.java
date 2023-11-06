package com.onehouse.api;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class AsyncHttpClientWithRetry {

  private final ScheduledExecutorService scheduler;
  private final int maxRetries;
  private final long retryDelayMillis;
  private final OkHttpClient okHttpClient;
  private static final long MAX_RETRY_DELAY_MILLIS = 10000; // 10seconds
  // TODO: fill failure error codes for which retry is not required
  // using mapping from:
  // https://chromium.googlesource.com/external/github.com/grpc/grpc/+/refs/tags/v1.21.4-pre1/doc/statuscodes.md
  private static final List<Integer> ACCEPTABLE_HTTP_FAILURE_STATUS_CODES = List.of(404, 400);

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
              public void onFailure(Call call, IOException e) {
                if (tryCount < maxRetries) {
                  scheduleRetry(request, tryCount, future);
                } else {
                  future.completeExceptionally(e);
                }
              }

              @Override
              public void onResponse(Call call, Response response) {
                if (!response.isSuccessful()
                    && !ACCEPTABLE_HTTP_FAILURE_STATUS_CODES.contains(response.code())
                    && tryCount < maxRetries) {
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
    // Exponential backoff with upper bound
    return (long) Math.min(MAX_RETRY_DELAY_MILLIS, retryDelayMillis * Math.pow(2, tryCount));
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
