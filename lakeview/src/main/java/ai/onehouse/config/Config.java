package ai.onehouse.config;

import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.OnehouseClientConfig;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;

public interface Config {
  ConfigVersion getVersion();

  FileSystemConfiguration getFileSystemConfiguration();

  OnehouseClientConfig getOnehouseClientConfig();

  String getMetadataExtractorConfigPath();

  MetadataExtractorConfig getMetadataExtractorConfig();
}
