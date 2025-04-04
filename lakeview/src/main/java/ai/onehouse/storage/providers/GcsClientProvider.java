package ai.onehouse.storage.providers;

import static ai.onehouse.constants.StorageConstants.GCP_RESOURCE_NAME_FORMAT;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import ai.onehouse.config.Config;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.GCSConfig;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
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
    this.gcsConfig =
        fileSystemConfiguration.getGcsConfig() != null
            ? fileSystemConfiguration.getGcsConfig()
            : GCSConfig.builder().build();
  }

  @VisibleForTesting
  Storage createGcsClient() {
    logger.debug("Instantiating GCS storage client");
    validateGcsConfig(gcsConfig);

    // Use Google Default ADC if serviceAccountJson not provided
    // https://cloud.google.com/docs/authentication/provide-credentials-adc
    if (gcsConfig.getGcpServiceAccountKeyPath().isPresent()) {
      StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
      try (FileInputStream serviceAccountStream = readAsStream()) {
        if (gcsConfig.getServiceAccountToImpersonate().isPresent()) {
          // Impersonate Service Account
          ImpersonatedCredentials impersonatedCredentials = ImpersonatedCredentials.create(
              GoogleCredentials.fromStream(serviceAccountStream),
              gcsConfig.getServiceAccountToImpersonate().get(),
              null,
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"),
              3600
          );
          storageOptionsBuilder.setCredentials(impersonatedCredentials);
        } else {
          storageOptionsBuilder.setCredentials(GoogleCredentials.fromStream(serviceAccountStream));
        }

        if (gcsConfig.getProjectId().isPresent()) {
          storageOptionsBuilder.setProjectId(gcsConfig.getProjectId().get());
        }

        return storageOptionsBuilder.build().getService();
      } catch (IOException e) {
        throw new RuntimeException("Error reading service account JSON key file", e);
      }
    }
    return StorageOptions.getDefaultInstance().getService();
  }

  public Storage getGcsClient() {
    if (gcsClient == null) {
      gcsClient = createGcsClient();
    }
    return gcsClient;
  }

  public void refreshClient() {
    gcsClient = createGcsClient();
  }

  private void validateGcsConfig(GCSConfig gcsConfig) {
    if (gcsConfig.getProjectId().isPresent()
        && !gcsConfig.getProjectId().get().matches(GCP_RESOURCE_NAME_FORMAT)) {
      throw new IllegalArgumentException(
          "Invalid GCP project ID: " + gcsConfig.getProjectId().get());
    }

    if (gcsConfig.getGcpServiceAccountKeyPath().isPresent()
        && StringUtils.isBlank(gcsConfig.getGcpServiceAccountKeyPath().get())) {
      throw new IllegalArgumentException(
          "Invalid GCP Service Account Key Path: " + gcsConfig.getGcpServiceAccountKeyPath().get());
    }
  }

  @VisibleForTesting
  FileInputStream readAsStream() throws FileNotFoundException {
    return new FileInputStream(gcsConfig.getGcpServiceAccountKeyPath().get());
  }
}
