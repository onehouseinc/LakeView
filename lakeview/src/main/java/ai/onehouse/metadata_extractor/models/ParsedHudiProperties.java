package ai.onehouse.metadata_extractor.models;

import ai.onehouse.api.models.request.TableType;
import ai.onehouse.constants.MetricsConstants;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nullable;

@Builder
@Value
public class ParsedHudiProperties {
  @NonNull String tableName;
  @NonNull TableType tableType;
  @Nullable MetricsConstants.MetadataUploadFailureReasons metadataUploadFailureReasons;
}
