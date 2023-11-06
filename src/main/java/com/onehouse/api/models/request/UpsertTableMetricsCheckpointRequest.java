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
public class UpsertTableMetricsCheckpointRequest {
  @NonNull private final String tableId;
  @NonNull private final String checkpoint;
  @NonNull private final List<String> filesUploaded;
  @NonNull private final CommitTimelineType commitTimelineType;
}
