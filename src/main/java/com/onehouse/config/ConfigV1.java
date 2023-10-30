package com.onehouse.config;

import com.onehouse.config.common.FileSystemConfiguration;
import com.onehouse.config.common.OnehouseClientConfig;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
class Database {
  @NonNull private String name;
  @NonNull private List<String> basePaths;
  private List<String> excludePaths;
}

@Builder
@Getter
@Jacksonized
class MetadataExtractorConfig {
  @NonNull private List<ParserConfig> parserConfig;
}

@Builder
@Getter
@Jacksonized
class ParserConfig {
  @NonNull private String lake;
  @NonNull private List<Database> databases;
}

@Builder
@Getter
@Jacksonized
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
