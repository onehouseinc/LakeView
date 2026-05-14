package ai.onehouse.config.models.configv1;

import ai.onehouse.api.models.request.TableFormat;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class Database {
  String name;
  @NonNull List<String> basePaths;
  /**
   * Physical table format of the tables under {@link #basePaths}. Null is interpreted as {@link
   * TableFormat#HUDI} for backward compatibility with YAMLs written before Iceberg support
   * existed. Customers with a mixed-format warehouse should split into separate {@link Database}
   * entries, one per format.
   */
  @Nullable TableFormat tableFormat;
  /**
   * Optional per-table hints keyed on the tableId encoded in {@link #basePaths} (the segment after
   * {@code #}). The control plane populates this for tables it has richer information about
   * (e.g. Iceberg tables discovered via AWS Glue, where the catalog already knows the current
   * {@code metadata.json} URI). Missing entries / missing map are normal — older YAML versions
   * predate this field, and tables without hints fall back to plain discovery.
   */
  @Nullable Map<String, TableHint> tableHints;
}
