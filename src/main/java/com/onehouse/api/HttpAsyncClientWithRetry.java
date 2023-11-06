package com.onehouse.api;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpAsyncClientWithRetry {

  private final ScheduledExecutorService scheduler;
  private final int maxRetries;
  private final long retryDelayMillis;
  private final OkHttpClient okHttpClient;

  public HttpAsyncClientWithRetry(
      int maxRetries, long retryDelayMillis, OkHttpClient okHttpClient) {
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.okHttpClient = okHttpClient;
  }

  public CompletableFuture<Response> makeRequestWithRetry(Request request) {
    return attemptRequest(request, 0);
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
                if (!response.isSuccessful() && tryCount < maxRetries) {
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
        (long) (retryDelayMillis * Math.pow(2, tryCount)),
        TimeUnit.MILLISECONDS); // Exponential backoff
  }

  // Make sure to shut down the scheduler when it's no longer needed
  public void shutdownScheduler() {
    scheduler.shutdown();
  }
}
