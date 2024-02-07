package com.onehouse.config;

import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.OnehouseClientConfig;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;

public interface Config {
  ConfigVersion getVersion();

  FileSystemConfiguration getFileSystemConfiguration();

  OnehouseClientConfig getOnehouseClientConfig();

  String getMetadataExtractorConfigPath();

  MetadataExtractorConfig getMetadataExtractorConfig();
}
