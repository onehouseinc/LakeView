package com.onehouse.api.models.response;

import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class InitializeTableMetricsCheckpointResponse extends ApiResponse {
  @Builder
  @Value
  @Jacksonized
  public static class InitializeSingleTableMetricsCheckpointResponse {
    String tableId;
    String error;
  }

  List<InitializeSingleTableMetricsCheckpointResponse> response;
}
