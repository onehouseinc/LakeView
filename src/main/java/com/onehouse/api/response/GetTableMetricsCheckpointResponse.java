package com.onehouse.api.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GetTableMetricsCheckpointResponse extends ApiResponse {
  private String checkpoint;
}
