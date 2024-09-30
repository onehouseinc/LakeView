package ai.onehouse.api.models.response;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GetTableMetricsCheckpointResponse extends ApiResponse {
  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TableMetadataCheckpoint {
    String tableId;
    String checkpoint;
  }

  private List<TableMetadataCheckpoint> checkpoints;
}
