package com.onehouse.config.common;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpKey;
}
