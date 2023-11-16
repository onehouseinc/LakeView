package com.onehouse.metadata_extractor.models;

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
  private String relativeTablePath;
}
