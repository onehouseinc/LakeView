package com.onehouse.api.response;

import lombok.Builder;
import lombok.NonNull;

@Builder
public class UpsertTableMetricsCheckpointResponse extends ApiResponse {
  @NonNull private final String checkpoint;
}
