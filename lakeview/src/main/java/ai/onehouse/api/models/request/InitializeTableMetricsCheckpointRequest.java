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
    // For Hudi: source of truth for COW vs MOR. For non-Hudi formats it is a meaningless
    // placeholder; the server discriminates on tableFormat first. Kept non-null to preserve
    // the existing wire contract; defaults to COPY_ON_WRITE for Iceberg tables.
    @NonNull TableType tableType;
    // Physical table format. Absent is interpreted by the server as HUDI for backward compat
    // with extractors that haven't been updated.
    @Nullable TableFormat tableFormat;
    String lakeName;
    String databaseName;
    String tableBasePath;
    @Nullable
    @JsonIgnore
    MetricsConstants.MetadataUploadFailureReasons failureReasons;
  }

  List<InitializeSingleTableMetricsCheckpointRequest> tables;
}
