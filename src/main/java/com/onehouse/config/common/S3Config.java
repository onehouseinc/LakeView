package com.onehouse.config.common;

import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class S3Config {
  @NonNull private String region;

  // optional to be used for quick testing
  private Optional<String> accessKey = Optional.empty();
  private Optional<String> accessSecret = Optional.empty();
}
