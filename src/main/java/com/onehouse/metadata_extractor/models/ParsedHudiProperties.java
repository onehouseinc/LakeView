package com.onehouse.metadata_extractor.models;

import com.onehouse.api.request.TableType;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class ParsedHudiProperties {
  @NonNull private final String tableName;
  @NonNull private final TableType tableType;
}
