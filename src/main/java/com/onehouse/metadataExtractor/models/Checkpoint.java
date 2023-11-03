package com.onehouse.metadataExtractor.models;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class Checkpoint {
  private final int batchId;
  @NonNull private final Instant checkpointTimestamp;
  @NonNull private final Boolean archivedCommitsProcessed;
  @NonNull private final String lastUploadedFile;
}
