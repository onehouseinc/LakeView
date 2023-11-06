package com.onehouse.config.configv1;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class Database {
  @NonNull private String name;
  @NonNull private List<String> basePaths;
}
