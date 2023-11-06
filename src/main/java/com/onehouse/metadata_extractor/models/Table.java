package com.onehouse.metadata_extractor.models;

import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class Table {
  String absoluteTableUri;
  String relativeTablePath;
  String databaseName;
  String lakeName;
}
