package com.onehouse.metadataExtractor.models;

import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class Table {
  private final String absoluteTableUrl;
  private final String relativeTablePath;
  private final String databaseName;
  private final String lakeName;
}
