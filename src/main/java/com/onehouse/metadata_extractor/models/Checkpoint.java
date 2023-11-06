package com.onehouse.metadata_extractor.models;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@NoArgsConstructor(force = true)
@AllArgsConstructor
public class Checkpoint implements Serializable {
  int batchId;
  @NonNull Instant checkpointTimestamp;
  @NonNull String lastUploadedFile;
  boolean archivedCommitsProcessed;
}
