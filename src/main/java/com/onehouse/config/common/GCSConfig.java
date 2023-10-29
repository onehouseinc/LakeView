package com.onehouse.config.common;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpKey;
}
