package com.onehouse.api.request;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class UpsertTableMetricsCheckpointRequest {
  @NonNull private final String tableBasePath;
  @NonNull private final String Checkpoint;
}
