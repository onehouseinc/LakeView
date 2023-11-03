package com.onehouse.api.response;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class GenerateCommitMetadataUploadUrlResponse extends ApiResponse {
  @NonNull private final List<String> uploadUrls;
}
