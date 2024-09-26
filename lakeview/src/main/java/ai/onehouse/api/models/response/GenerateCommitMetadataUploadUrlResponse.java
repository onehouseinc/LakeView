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
public class GenerateCommitMetadataUploadUrlResponse extends ApiResponse {
  private List<String> uploadUrls;
}
