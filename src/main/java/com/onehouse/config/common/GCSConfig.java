package com.onehouse.config.common;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
@EqualsAndHashCode
public class GCSConfig {
  @NonNull private String projectId;
  @NonNull private String gcpServiceAccountKeyPath;
}
