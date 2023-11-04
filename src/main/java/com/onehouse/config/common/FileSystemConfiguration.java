package com.onehouse.config.common;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@EqualsAndHashCode
public class FileSystemConfiguration {
  private S3Config s3Config;
  private GCSConfig gcsConfig;
}
