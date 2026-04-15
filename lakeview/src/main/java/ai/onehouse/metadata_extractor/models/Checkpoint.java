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
  // Last LSM manifest version (from .hoodie/timeline/history/_version_) that has been
  // mirrored to the backend. Used for V2 (Hudi 1.x / table version >= 8) archived timeline
  // upload, where archives live in an LSM tree and filename-based checkpointing breaks
  // because compaction rewrites the file set. V1 archived path leaves this at 0.
  @Builder.Default int lastArchivedManifestVersion = 0;
}
