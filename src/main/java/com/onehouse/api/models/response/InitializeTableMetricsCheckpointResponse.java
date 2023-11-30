package com.onehouse.api.models.response;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class InitializeTableMetricsCheckpointResponse extends ApiResponse {
  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class InitializeSingleTableMetricsCheckpointResponse {
    String tableId;
    String error;
  }

  List<InitializeSingleTableMetricsCheckpointResponse> response;
}
