package com.onehouse.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompletableFutureUtils {
  public static <T> List<CompletableFuture<T>> applyTimeouts(
      List<CompletableFuture<T>> futures, long timeout, TimeUnit timeUnit, String timeoutMessage) {

    return futures.stream()
        .map(future -> applyTimeout(future, timeout, timeUnit, timeoutMessage))
        .collect(Collectors.toList());
  }

  public static <T> CompletableFuture<T> applyTimeout(
      CompletableFuture<T> future, long timeout, TimeUnit timeUnit, String timeoutMessage) {

    CompletableFuture<T> timeoutFuture = failAfter(timeout, timeUnit, timeoutMessage);

    return future.applyToEither(timeoutFuture, Function.identity());
  }

  private static <T> CompletableFuture<T> failAfter(
      long timeout, TimeUnit unit, String timeoutMessage) {
    CompletableFuture<T> future = new CompletableFuture<>();
    Executors.newScheduledThreadPool(1)
        .schedule(
            () -> {
              TimeoutException ex = new TimeoutException("Timeout after " + timeout + " " + unit);
              log.error(timeoutMessage);
              future.completeExceptionally(ex);
            },
            timeout,
            unit);
    return future;
  }
}
