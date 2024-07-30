package com.onehouse.config.models.common;

import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class S3Config {
  @NonNull private String region;

  // optional to be used for quick testing
  @Builder.Default private Optional<String> accessKey = Optional.empty();
  @Builder.Default private Optional<String> accessSecret = Optional.empty();
  @Builder.Default private Optional<String> endpoint = Optional.empty();
  @Builder.Default private Optional<Boolean> forcePathStyle = Optional.empty();
}
