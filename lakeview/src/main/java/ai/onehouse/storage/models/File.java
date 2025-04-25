package ai.onehouse.storage.models;

import java.time.Instant;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class File {
  @NonNull String filename; // filename does not include the path prefix
  @NonNull Instant lastModifiedAt;
  boolean isDirectory;
}
