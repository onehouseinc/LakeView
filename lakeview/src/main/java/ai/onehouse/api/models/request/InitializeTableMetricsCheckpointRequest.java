package ai.onehouse.api.models.request;

import java.util.List;

import ai.onehouse.constants.MetricsConstants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

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
    @Nullable
    @JsonIgnore
    MetricsConstants.MetadataUploadFailureReasons failureReasons;
  }

  List<InitializeSingleTableMetricsCheckpointRequest> tables;
}
