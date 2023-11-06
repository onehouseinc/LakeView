package com.onehouse.config.configv1;

import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class MetadataExtractorConfig {
  @NonNull private List<ParserConfig> parserConfig;
  @Builder.Default private Optional<List<String>> pathsToExclude = Optional.empty();
}
