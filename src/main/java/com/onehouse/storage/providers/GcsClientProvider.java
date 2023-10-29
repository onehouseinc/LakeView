package com.onehouse.storage.providers;

import static com.onehouse.storage.storageConstants.GCP_RESOURCE_NAME_FORMAT;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigV1;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.GCSConfig;
import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class GcsClientProvider {

  private final Storage gcsClient;

  @Inject
  public GcsClientProvider(@Nonnull Config config) {
    FileSystemConfiguration fileSystemConfiguration =
        ((ConfigV1) config).getFileSystemConfiguration();
    validateGcsConfig(fileSystemConfiguration.getGcsConfig());
    // TODO: support gcpKey based auth
    this.gcsClient =
        StorageOptions.newBuilder()
            .setProjectId(fileSystemConfiguration.getGcsConfig().getProjectId())
            .build()
            .getService();
  }

  private void validateGcsConfig(GCSConfig gcsConfig) {
    if (gcsConfig == null) {
      throw new IllegalArgumentException("Gcs config not found");
    }

    if (!gcsConfig.getProjectId().matches(GCP_RESOURCE_NAME_FORMAT)) {
      throw new IllegalArgumentException("Invalid GCP project ID: " + gcsConfig.getProjectId());
    }
  }
}
