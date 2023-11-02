package com.onehouse.config.configV1;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class Database {
  @NonNull private String name;
  @NonNull private List<String> basePaths;
}
