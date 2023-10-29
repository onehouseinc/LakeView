package com.onehouse.config.common;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
public class FileSystemConfiguration {
  private S3Config s3Config;
  private GCSConfig gcsConfig;
}
