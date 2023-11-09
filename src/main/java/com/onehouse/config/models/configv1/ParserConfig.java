package com.onehouse.config.models.configv1;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class ParserConfig {
  @NonNull private String lake;
  @NonNull private List<Database> databases;
}
