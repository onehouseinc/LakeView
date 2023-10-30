package com.onehouse.metadataExtractor.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class Table {
  private final String tablePath;
  private final String databaseName;
  private final String lakeName;
}
