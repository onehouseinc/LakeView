package com.onehouse.config.common;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class OnehouseClientConfig {
  @NonNull private String projectId;
  @NonNull private String apiKey;
  @NonNull private String apiSecret;
  @NonNull private String region;
  @NonNull private String userUuid;
}
