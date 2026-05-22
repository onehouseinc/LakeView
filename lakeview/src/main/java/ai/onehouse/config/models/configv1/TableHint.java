package ai.onehouse.config.models.configv1;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Optional per-table hints supplied by the control plane in the parser YAML, keyed on tableId in
 * {@link Database#getTableHints()}. Hints are forward-compatible: older LakeView versions that
 * don't know about a field deserialize the rest fine; new fields default to null.
 */
@Builder
@Value
@Jacksonized
public class TableHint {
  /**
   * For Iceberg tables registered in an external catalog (e.g. AWS Glue), the URI of the current
   * {@code metadata.json} that the catalog points at. When present, the extractor can PUT this
   * file directly instead of listing the {@code metadata/} folder to find the latest. Empty /
   * null means "no hint — fall back to listing".
   */
  @Nullable String metadataLocationHint;
}
