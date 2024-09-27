package ai.onehouse.metadata_extractor.models;

import ai.onehouse.api.models.request.TableType;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class ParsedHudiProperties {
  @NonNull String tableName;
  @NonNull TableType tableType;
}
