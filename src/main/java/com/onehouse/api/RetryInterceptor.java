package com.onehouse.api;

import java.io.IOException;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

public class RetryInterceptor implements Interceptor {

  private final int maxRetries;
  private final long retryDelayMillis;

  public RetryInterceptor(int maxRetries, long retryDelayMillis) {
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
  }

  @Override
  public Response intercept(Interceptor.Chain chain) throws IOException {
    Request request = chain.request();
    Response response = null;
    IOException exception = null;

    for (int i = 0; i < maxRetries; i++) {
      try {
        response = chain.proceed(request);
        if (response.isSuccessful()) {
          return response;
        }
      } catch (IOException e) {
        exception = e;
      }

      // Wait for a while before retrying
      try {
        Thread.sleep(retryDelayMillis);
      } catch (InterruptedException ignored) {
      }
    }

    // Throw the last exception if we've exhausted retries
    if (exception != null) {
      throw exception;
    } else {
      // Or return the last response, which will likely be an error response
      return response;
    }
  }
}
