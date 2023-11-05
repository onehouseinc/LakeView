package com.onehouse.api.models.request;

import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class UpsertTableMetricsCheckpointRequest {
  @NonNull private final UUID tableId;
  @NonNull private final String checkpoint;
  @NonNull private final List<String> filesUploaded;
  @NonNull private final CommitTimelineType commitTimelineType;
}
