package com.onehouse.metadataExtractor.models;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class Checkpoint {
  private final int batchId;
  @NonNull private final Instant checkpoint;
  @NonNull private final Boolean isArchivedCommitsProcessed;
  @NonNull private final String lastUploadedFile;
}
