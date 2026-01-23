package ai.onehouse.config.models.common;

import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
@EqualsAndHashCode
public class AzureConfig {
  @NonNull private String accountName;

  // Optional authentication methods
  // Option 1: Account Key (for dev/testing, never expires)
  @Builder.Default private Optional<String> accountKey = Optional.empty();

  // Option 2: Connection String (alternative to account key)
  @Builder.Default private Optional<String> connectionString = Optional.empty();

  // Option 3: Service Principal
  @Builder.Default private Optional<String> tenantId = Optional.empty();
  @Builder.Default private Optional<String> clientId = Optional.empty();
  @Builder.Default private Optional<String> clientSecret = Optional.empty();
}
