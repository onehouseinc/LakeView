package com.onehouse.config.common;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@EqualsAndHashCode
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpServiceAccountKeyPath;
}
