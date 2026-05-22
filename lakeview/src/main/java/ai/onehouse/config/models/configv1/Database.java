package ai.onehouse.config.models.configv1;

import ai.onehouse.api.models.request.TableFormat;
import java.util.List;
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
   * TableFormat#HUDI} for backward compatibility with YAMLs written before format-aware
   * discovery existed. Customers with a mixed-format warehouse should split into separate {@link
   * Database} entries, one per format.
   */
  @Nullable TableFormat tableFormat;
}
