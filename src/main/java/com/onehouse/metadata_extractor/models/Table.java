package com.onehouse.metadata_extractor.models;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class Table {
  @NonNull private final String absoluteTableUri;
  @NonNull private final String databaseName;
  @NonNull private final String lakeName;
  private String relativeTablePath;
}
