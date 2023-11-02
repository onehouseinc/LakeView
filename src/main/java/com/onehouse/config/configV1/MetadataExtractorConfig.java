package com.onehouse.config.configV1;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class MetadataExtractorConfig {
  @NonNull private List<ParserConfig> parserConfig;
  private Optional<List<String>> pathsToExclude = Optional.empty();
}
