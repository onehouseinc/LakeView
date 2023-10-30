package com.onehouse.api.response;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
public class UpsertTableMetricsCheckpointResponse {
  @NonNull private final String checkpoint;
}
