package ai.onehouse.storage.models;

import java.io.InputStream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class FileStreamData {
  @NonNull InputStream inputStream;
  long fileSize;
}
