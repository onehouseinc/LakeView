package com.onehouse.config.common;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FileSystemConfiguration {
  private S3Config s3Config;
  private GCSConfig gcsConfig;
}
