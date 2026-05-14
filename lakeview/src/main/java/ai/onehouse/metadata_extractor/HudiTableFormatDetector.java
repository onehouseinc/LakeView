package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.models.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A directory is a Hudi table root if it contains a {@code .hoodie/} folder. */
public class HudiTableFormatDetector implements TableFormatDetector {
  @Override
  public TableFormat format() {
    return TableFormat.HUDI;
  }

  @Override
  public CompletableFuture<Boolean> matches(String path, List<File> listedFiles) {
    return CompletableFuture.completedFuture(
        listedFiles.stream().anyMatch(file -> file.getFilename().startsWith(HOODIE_FOLDER_NAME)));
  }
}
