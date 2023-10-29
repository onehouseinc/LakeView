package com.onehouse;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeModule.class);
  private static final int IO_WORKLOAD_NUM_THREAD_MULTIPLIER = 5;

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface ApplicationExecutor {}

  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface SharedIOExecutor {}

  @Provides
  @Singleton
  static OkHttpClient providesOkHttpClient(@SharedIOExecutor ExecutorService executorService) {
    Dispatcher dispatcher = new Dispatcher(executorService);
    return new OkHttpClient.Builder().dispatcher(dispatcher).build();
  }

  // TODO: remove if unused
  @Provides
  @Singleton
  @ApplicationExecutor
  static ExecutorService providesApplicationExecutor() {
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
        Runtime.getRuntime().availableProcessors(),
        new ApplicationThreadFactory(),
        (thread, throwable) -> {
          logger.error(
              String.format("Uncaught exception in a thread (%s)", thread.getName()), throwable);
        },
        // NOTE: It's squarely important to make sure
        // that `asyncMode` is true in async applications
        true);
  }

  @Provides
  @Singleton
  @SharedIOExecutor
  static ExecutorService providesSharedIOExecutor() {
    class ApplicationThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
      private static final String THREAD_GROUP_NAME_TEMPLATE = "gw-shared-io-%d";
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
            * IO_WORKLOAD_NUM_THREAD_MULTIPLIER, // more threads in IO thread pool due to nature of
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
}
