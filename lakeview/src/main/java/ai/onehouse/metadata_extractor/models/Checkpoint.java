package ai.onehouse.metadata_extractor.models;

import java.io.Serializable;
import java.time.Instant;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder(toBuilder = true)
@Value
@Jacksonized
public class Checkpoint implements Serializable {
  int batchId;
  @NonNull Instant checkpointTimestamp;
  @NonNull String lastUploadedFile;
  String firstIncompleteCommitFile;
  boolean archivedCommitsProcessed;
}
