package com.onehouse.storage.models;

import java.time.Instant;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class File {
  @NonNull String filename;
  @NonNull Instant lastModifiedAt;
  boolean isDirectory;
}
