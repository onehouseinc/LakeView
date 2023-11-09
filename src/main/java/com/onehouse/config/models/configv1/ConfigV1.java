package com.onehouse.config.models.configv1;

import com.onehouse.config.Config;
import com.onehouse.config.ConfigVersion;
import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.OnehouseClientConfig;
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
  @NonNull private MetadataExtractorConfig metadataExtractorConfig;

  @Override
  public ConfigVersion getVersion() {
    return ConfigVersion.valueOf(version);
  }
}
