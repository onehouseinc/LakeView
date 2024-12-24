package ai.onehouse.config.models.common;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Setter
@Jacksonized
@EqualsAndHashCode
public class OnehouseClientConfig {
  @Nullable private String projectId;
  @Nullable private String requestId;
  @Nullable private String region;
  @Nullable private String apiKey;
  @Nullable private String apiSecret;
  @Nullable private String userId;
  @Nullable private String file;
  @Nullable private Boolean maintenanceMode;
}
