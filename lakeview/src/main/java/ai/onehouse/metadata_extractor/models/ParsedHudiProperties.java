package ai.onehouse.metadata_extractor.models;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_VERSION_DEFAULT;
import static ai.onehouse.constants.MetadataExtractorConstants.TIMELINE_LAYOUT_VERSION_DEFAULT;

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
  @Builder.Default int tableVersion = HOODIE_TABLE_VERSION_DEFAULT;
  @Builder.Default int timelineLayoutVersion = TIMELINE_LAYOUT_VERSION_DEFAULT;
}
