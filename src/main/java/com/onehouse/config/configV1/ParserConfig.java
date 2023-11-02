package com.onehouse.config.configV1;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class ParserConfig {
  @NonNull private String lake;
  @NonNull private List<Database> databases;
}
