package com.onehouse.storage.models;

import java.time.Instant;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
@EqualsAndHashCode
public class File {
  @NonNull private String filename;
  @NonNull private Instant lastModifiedAt;
  @NonNull private Boolean isDirectory;
}
