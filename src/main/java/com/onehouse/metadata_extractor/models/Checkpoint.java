package com.onehouse.metadata_extractor.models;

import java.io.Serializable;
import java.time.Instant;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class Checkpoint implements Serializable {
  int batchId;
  @NonNull Instant checkpointTimestamp;
  @NonNull String lastUploadedFile;
  boolean archivedCommitsProcessed;
  String continuationToken;
}
