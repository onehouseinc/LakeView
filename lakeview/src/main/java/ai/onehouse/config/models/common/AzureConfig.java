package ai.onehouse.config.models.common;

import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Getter
@EqualsAndHashCode
public class AzureConfig {
  @Builder.Default private Optional<String> storageAccountName = Optional.empty();
  @Builder.Default private Optional<String> subscriptionId = Optional.empty();
  @Builder.Default private Optional<String> tenantId = Optional.empty();
}

