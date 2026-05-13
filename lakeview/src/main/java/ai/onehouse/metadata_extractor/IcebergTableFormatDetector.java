package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.ICEBERG_METADATA_FOLDER_NAME;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.models.File;
import java.util.List;

/**
 * A directory is an Iceberg table root if it contains a {@code metadata/} folder. The folder
 * itself holds the {@code *.metadata.json} pointer files plus manifest list / manifest avros;
 * presence alone is sufficient to identify the table root since Iceberg always emits {@code
 * metadata/} on the first commit.
 */
public class IcebergTableFormatDetector implements TableFormatDetector {
  @Override
  public TableFormat format() {
    return TableFormat.ICEBERG;
  }

  @Override
  public boolean matches(List<File> listedFiles) {
    return listedFiles.stream()
        .anyMatch(file -> file.isDirectory() && ICEBERG_METADATA_FOLDER_NAME.equals(file.getFilename()));
  }
}
