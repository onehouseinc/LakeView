package com.onehouse.config.common;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class S3Config {
  @NonNull private String region;

  // optional to be used for quick testing
  @Builder.Default private Optional<String> accessKey = Optional.empty();
  @Builder.Default private Optional<String> accessSecret = Optional.empty();
}
