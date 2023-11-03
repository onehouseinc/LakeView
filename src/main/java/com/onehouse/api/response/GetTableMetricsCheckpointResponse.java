package com.onehouse.api.response;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class GetTableMetricsCheckpointResponse extends ApiResponse {
  @NonNull private final String checkpoint;
}
