package com.onehouse.api.models.request;

import java.util.UUID;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class InitializeTableMetricsCheckpointRequest {
  @NonNull UUID tableId;
  @NonNull String tableName;
  @NonNull TableType tableType;
  String lakeName;
  String databaseName;
}
