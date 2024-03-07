package com.onehouse.api.models.request;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class InitializeTableMetricsCheckpointRequest {
  @Builder
  @Value
  @Jacksonized
  public static class InitializeSingleTableMetricsCheckpointRequest {
    @NonNull String tableId;
    @NonNull String tableName;
    @NonNull TableType tableType;
    String lakeName;
    String databaseName;
    String tableBasePath;
  }

  List<InitializeSingleTableMetricsCheckpointRequest> tables;
}
