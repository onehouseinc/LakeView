package com.onehouse.metadataExtractor.models;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Setter
@Jacksonized
public class Checkpoint {
  private final Instant lastProcessedTime;
  private final String lastSuccessfulCommit;
  private final Boolean isArchivedCommitsProcessed;
}
