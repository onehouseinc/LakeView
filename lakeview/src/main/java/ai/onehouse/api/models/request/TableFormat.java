package ai.onehouse.api.models.request;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Physical observed-table format. Wire-serialized to the protobuf enum names defined in
 * lake/table.proto's {@code TableFormat} so the control plane's protobuf-java-util JSON parser
 * accepts the value on RPCs like {@code InitializeTableMetricsCheckpoint}. Without these
 * {@code @JsonProperty} aliases, Jackson would emit the short Java identifier ({@code "ICEBERG"})
 * and the server-side parser — which requires the canonical proto enum name
 * ({@code "TABLE_FORMAT_ICEBERG"}) — would silently drop the value and persist the checkpoint
 * with the proto enum-zero default ({@code TABLE_FORMAT_INVALID}), routing iceberg uploads
 * through the Hudi back-compat fallback.
 */
public enum TableFormat {
  @JsonProperty("TABLE_FORMAT_HUDI")
  HUDI,
  @JsonProperty("TABLE_FORMAT_ICEBERG")
  ICEBERG
}
