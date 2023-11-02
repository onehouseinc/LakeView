package com.onehouse.api.response;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class GetTableMetricsCheckpointResponse extends ApiResponse {
  @NonNull private final String checkpoint;
}
