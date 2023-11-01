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
  private final int batchId;
  private final Instant checkpoint;
  private final Boolean isArchivedCommitsProcessed;
  private final String lastUploadedFile;
}
