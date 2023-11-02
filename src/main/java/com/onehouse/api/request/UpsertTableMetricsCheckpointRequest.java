package com.onehouse.api.request;

import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class UpsertTableMetricsCheckpointRequest {
  @NonNull private final UUID tableId;
  @NonNull private final String Checkpoint;
  @NonNull private final List<String> filesUploaded;
  @NonNull private final CommitTimelineType commitTimelineType;
}
