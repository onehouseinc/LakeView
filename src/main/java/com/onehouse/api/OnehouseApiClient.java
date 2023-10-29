package com.onehouse.api;

import com.google.inject.Inject;
import javax.annotation.Nonnull;
import okhttp3.OkHttpClient;

public class OnehouseApiClient {
  private final OkHttpClient okHttpClient;

  @Inject
  public OnehouseApiClient(@Nonnull OkHttpClient okHttpClient) {
    this.okHttpClient = okHttpClient;
  }
}
