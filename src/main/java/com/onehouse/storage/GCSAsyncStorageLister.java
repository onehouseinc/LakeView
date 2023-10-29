package com.onehouse.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigV1;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.GCSConfig;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GCSAsyncStorageLister implements AsyncStorageLister {
  private final Storage storageClient;
  private final ExecutorService executorService;
  // gcs path format "gs:// [bucket] /path/to/file"
  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("^gs://([^/]+)/.*");

  @Inject
  public GCSAsyncStorageLister(@Nonnull ExecutorService executorService, @Nonnull Config config) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    validateGcsConfig(fileSystemConfiguration.getGcsConfig());
    //TODO: support gcpKey based auth
    this.storageClient =
        StorageOptions.newBuilder()
            .setProjectId(fileSystemConfiguration.getGcsConfig().getProjectId())
            .build()
            .getService();
    this.executorService = executorService;
  }

  @Override
  public CompletableFuture<List<String>> listFiles(String gcsPath) {
    return CompletableFuture.supplyAsync(
        () -> {
          Bucket bucket = storageClient.get(getGcsBucketNameFromPath(gcsPath));
          Iterable<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(gcsPath)).iterateAll();
          return StreamSupport.stream(blobs.spliterator(), false)
              .map(Blob::getName)
              .collect(Collectors.toList());
        },
        executorService);
  }

  private String getGcsBucketNameFromPath(String gcsPath) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(gcsPath);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Invalid GCS path: " + gcsPath);
  }

  private void validateGcsConfig(GCSConfig gcsConfig) {
    if (gcsConfig == null) {
      throw new IllegalArgumentException("Gcs config not found");
    }
    // https://cloud.google.com/compute/docs/naming-resources#resource-name-format
    if (!gcsConfig.getProjectId().matches("^[a-z]([-a-z0-9]*[a-z0-9])$")) {
      throw new IllegalArgumentException("Invalid GCP project ID: " + gcsConfig.getProjectId());
    }
  }
}
