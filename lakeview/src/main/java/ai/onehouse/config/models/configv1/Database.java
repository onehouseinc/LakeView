package ai.onehouse.config.models.configv1;

import java.util.List;
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
}
