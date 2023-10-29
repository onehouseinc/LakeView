package com.onehouse.config.common;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
public class OnehouseClientConfig {
  @NonNull private String projectId;
  @NonNull private String apiKey;
  @NonNull private String apiSecret;
  @NonNull private String userUuid;
}
