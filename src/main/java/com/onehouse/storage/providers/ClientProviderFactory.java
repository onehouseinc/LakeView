package com.onehouse.storage.providers;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

public class ClientProviderFactory {
  private final Config config;
  private final ExecutorService executorService;
  private GcsClientProvider gcsClientProvider;
  private S3AsyncClientProvider s3AsyncClientProvider;

  @Inject
  public ClientProviderFactory(@Nonnull Config config, @Nonnull ExecutorService executorService) {
    this.config = config;
    this.executorService = executorService;
  }

  public GcsClientProvider getGcsClientProvider() {
    if (gcsClientProvider == null) {
      gcsClientProvider = new GcsClientProvider(config);
    }
    return gcsClientProvider;
  }

  public S3AsyncClientProvider getS3AsyncClientProvider() {
    if (s3AsyncClientProvider == null) {
      s3AsyncClientProvider = new S3AsyncClientProvider(config, executorService);
    }
    return s3AsyncClientProvider;
  }
}
