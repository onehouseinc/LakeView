package com.onehouse.api.response;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
public class GetTableMetricsCheckpointResponse {
  @NonNull private final String checkpoint;
}
