package com.onehouse.config;

import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.OnehouseClientConfig;

public interface Config {
  ConfigVersion getVersion();

  FileSystemConfiguration getFileSystemConfiguration();

  OnehouseClientConfig getOnehouseClientConfig();
}
