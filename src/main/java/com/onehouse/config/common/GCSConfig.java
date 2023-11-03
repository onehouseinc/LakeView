package com.onehouse.config.common;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Setter
@Jacksonized
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpServiceAccountKeyPath;
}
