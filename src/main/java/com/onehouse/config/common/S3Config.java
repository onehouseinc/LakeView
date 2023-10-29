package com.onehouse.config.common;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
public class S3Config {
  @NonNull private String region;
  // optional to be used for quick testing
  private String accessKey;
  private String accessSecret;
}
