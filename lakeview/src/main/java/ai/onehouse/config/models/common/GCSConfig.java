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
public class GCSConfig {
  @Builder.Default private Optional<String> projectId = Optional.empty();
  @Builder.Default private Optional<String> gcpServiceAccountKeyPath = Optional.empty();
  @Builder.Default private Optional<String> serviceAccountToImpersonate = Optional.empty();
}
