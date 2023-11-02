package com.onehouse.config.configV1;

import com.onehouse.config.Config;
import com.onehouse.config.ConfigVersion;
import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.OnehouseClientConfig;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
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
