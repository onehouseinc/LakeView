package com.onehouse.metadata_extractor.models;

import com.onehouse.api.request.TableType;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class ParsedHudiProperties {
  @NonNull String tableName;
  @NonNull TableType tableType;
}
