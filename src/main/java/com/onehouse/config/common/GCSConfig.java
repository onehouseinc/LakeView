package com.onehouse.config.common;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpKey;
}
