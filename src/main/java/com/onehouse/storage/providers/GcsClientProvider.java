package com.onehouse.storage.providers;

import static com.onehouse.storage.StorageConstants.GCP_RESOURCE_NAME_FORMAT;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.GCSConfig;
import java.io.FileInputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class GcsClientProvider {
  private final GCSConfig gcsConfig;
  private static Storage gcsClient;
  private static final Logger logger = LoggerFactory.getLogger(GcsClientProvider.class);

  @Inject
  public GcsClientProvider(@Nonnull Config config) {
    FileSystemConfiguration fileSystemConfiguration = config.getFileSystemConfiguration();
    this.gcsConfig = fileSystemConfiguration.getGcsConfig();
  }

  protected Storage createGcsClient() {
    logger.debug("Instantiating GCS storage client");
    validateGcsConfig(gcsConfig);
    try (FileInputStream serviceAccountStream =
        new FileInputStream(gcsConfig.getGcpServiceAccountKeyPath())) {
      return StorageOptions.newBuilder()
          .setCredentials(GoogleCredentials.fromStream(serviceAccountStream))
          .setProjectId(gcsConfig.getProjectId())
          .build()
          .getService();
    } catch (IOException e) {
      throw new RuntimeException("Error reading service account JSON key file", e);
    }
  }

  public Storage getGcsClient() {
    if (gcsClient == null) {
      gcsClient = createGcsClient();
    }
    return gcsClient;
  }

  private void validateGcsConfig(GCSConfig gcsConfig) {
    if (gcsConfig == null) {
      throw new IllegalArgumentException("Gcs config not found");
    }

    if (!gcsConfig.getProjectId().matches(GCP_RESOURCE_NAME_FORMAT)) {
      throw new IllegalArgumentException("Invalid GCP project ID: " + gcsConfig.getProjectId());
    }

    if (gcsConfig.getGcpServiceAccountKeyPath().isBlank()) {
      throw new IllegalArgumentException(
          "Invalid GCP Service Account Key Path: " + gcsConfig.getGcpServiceAccountKeyPath());
    }
  }
}
