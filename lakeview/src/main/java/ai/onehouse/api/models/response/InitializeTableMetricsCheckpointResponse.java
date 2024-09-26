package ai.onehouse.api.models.response;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class InitializeTableMetricsCheckpointResponse extends ApiResponse {
  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  @ToString
  public static class InitializeSingleTableMetricsCheckpointResponse {
    String tableId;
    String error;
  }

  List<InitializeSingleTableMetricsCheckpointResponse> response;
}
