package ai.onehouse.metadata_extractor.models;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_TABLE_VERSION_DEFAULT;
import static ai.onehouse.constants.MetadataExtractorConstants.TIMELINE_LAYOUT_VERSION_DEFAULT;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class Table {
  @NonNull private final String absoluteTableUri;
  private final String databaseName;
  private final String lakeName;
  private String tableId;
  @Builder.Default private final int tableVersion = HOODIE_TABLE_VERSION_DEFAULT;
  @Builder.Default private final int timelineLayoutVersion = TIMELINE_LAYOUT_VERSION_DEFAULT;
}
