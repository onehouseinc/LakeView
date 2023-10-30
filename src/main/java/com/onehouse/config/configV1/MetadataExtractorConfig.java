package com.onehouse.config.configV1;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class MetadataExtractorConfig {
  @NonNull private List<ParserConfig> parserConfig;
}
