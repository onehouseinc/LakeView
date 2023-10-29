package com.onehouse.storage.models;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class File {
  @NonNull private String filename;
  @NonNull private Instant createdAt;
}
