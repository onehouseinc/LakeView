package com.onehouse.api.models.request;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Value
public class GenerateCommitMetadataUploadUrlRequest {
  @NonNull private final String tableId;
  @NonNull private final CommitTimelineType commitTimelineType;
  @NonNull private final List<String> commitInstants;
}
