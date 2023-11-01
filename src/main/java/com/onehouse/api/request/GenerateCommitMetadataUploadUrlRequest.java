package com.onehouse.api.request;

import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class GenerateCommitMetadataUploadUrlRequest {
  @NonNull private final UUID tableId;
  @NonNull private final CommitTimelineType commitTimelineType;
  @NonNull private final List<String> commitInstants;
}
