package ai.onehouse.config.models.configv1;

import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigVersion;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.OnehouseClientConfig;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class ConfigV1 implements Config {
  @NonNull private String version;
  @NonNull private OnehouseClientConfig onehouseClientConfig;
  @NonNull private FileSystemConfiguration fileSystemConfiguration;

  // If metadataExtractorConfigPath is provided, it overrides metadataExtractorConfig. If not
  // provided, it is mandatory to pass metadataExtractorConfig.
  private String metadataExtractorConfigPath;
  private MetadataExtractorConfig metadataExtractorConfig;

  @Override
  public ConfigVersion getVersion() {
    return ConfigVersion.valueOf(version);
  }
}
