package com.onehouse.api.models.request;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Value
public class UpsertTableMetricsCheckpointRequest {
  @NonNull private final String tableId;
  @NonNull private final String checkpoint;
  @NonNull private final List<String> filesUploaded;
  @NonNull private final List<UploadedFile> uploadedFiles;
  @NonNull private final CommitTimelineType commitTimelineType;
}
