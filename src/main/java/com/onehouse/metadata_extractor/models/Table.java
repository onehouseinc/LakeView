package com.onehouse.metadata_extractor.models;

import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class Table {
  private final String absoluteTableUri;
  private final String relativeTablePath;
  private final String databaseName;
  private final String lakeName;
}
