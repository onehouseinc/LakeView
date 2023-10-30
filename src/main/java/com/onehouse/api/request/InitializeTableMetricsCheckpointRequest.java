package com.onehouse.api.request;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class InitializeTableMetricsCheckpointRequest {
  @NonNull private final String tableBasePath;
  @NonNull private final String tableName;
  private final String lakeName;
  private final String databaseName;
  private final String tableType; // MOR | COW
}
