package com.onehouse.api.models.request;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class GenerateCommitMetadataUploadUrlRequest {
  @NonNull private final String tableId;
  @NonNull private final CommitTimelineType commitTimelineType;
  @NonNull private final List<String> commitInstants;
}
