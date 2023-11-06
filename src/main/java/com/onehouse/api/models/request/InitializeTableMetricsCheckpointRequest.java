package com.onehouse.api.models.request;

import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class InitializeTableMetricsCheckpointRequest {
  @NonNull private final UUID tableId;
  @NonNull private final String tableBasePath;
  @NonNull private final String tableName;
  @NonNull private final TableType tableType;
  private final String lakeName;
  private final String databaseName;
}