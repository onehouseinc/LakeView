package com.onehouse.storage.providers;

import static com.onehouse.storage.StorageConstants.GCP_RESOURCE_NAME_FORMAT;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.GCSConfig;
import com.onehouse.config.configV1.ConfigV1;
import java.io.FileInputStream;
import java.io.IOException;
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
    try (FileInputStream serviceAccountStream =
        new FileInputStream(fileSystemConfiguration.getGcsConfig().getGcpKey())) {
      this.gcsClient =
          StorageOptions.newBuilder()
              .setCredentials(GoogleCredentials.fromStream(serviceAccountStream))
              .setProjectId(fileSystemConfiguration.getGcsConfig().getProjectId())
              .build()
              .getService();
    } catch (IOException e) {
      throw new RuntimeException("Error reading service account JSON key file", e);
    }
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
