package ai.onehouse.config.models.common;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
@EqualsAndHashCode
public class FileSystemConfiguration {
  private S3Config s3Config;
  private GCSConfig gcsConfig;
}
