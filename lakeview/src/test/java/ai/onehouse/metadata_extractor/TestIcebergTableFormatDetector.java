package ai.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.models.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TestIcebergTableFormatDetector {
  private final IcebergTableFormatDetector detector = new IcebergTableFormatDetector();

  @Test
  void declaresIcebergFormat() {
    assertEquals(TableFormat.ICEBERG, detector.format());
  }

  @Test
  void matchesWhenMetadataDirectoryPresent() {
    assertTrue(
        detector.matches(
            Arrays.asList(file("metadata", true), file("data", true))));
  }

  @Test
  void doesNotMatchOnFileNamedMetadata() {
    // A non-directory entry named "metadata" must not be confused with the metadata/ folder.
    assertFalse(detector.matches(Collections.singletonList(file("metadata", false))));
  }

  @Test
  void doesNotMatchOnHudiLayout() {
    assertFalse(
        detector.matches(
            Arrays.asList(file(".hoodie", true), file("part-0.parquet", false))));
  }

  private static File file(String name, boolean dir) {
    return File.builder().filename(name).isDirectory(dir).lastModifiedAt(Instant.EPOCH).build();
  }
}
