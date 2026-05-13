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

class TestHudiTableFormatDetector {
  private static final String ANY_PATH = "s3://bucket/db/t/";
  private final HudiTableFormatDetector detector = new HudiTableFormatDetector();

  @Test
  void declaresHudiFormat() {
    assertEquals(TableFormat.HUDI, detector.format());
  }

  @Test
  void matchesWhenHoodieFolderPresent() {
    assertTrue(
        detector
            .matches(
                ANY_PATH,
                Arrays.asList(file(".hoodie", true), file("part-0.parquet", false)))
            .join());
  }

  @Test
  void doesNotMatchWhenAbsent() {
    assertFalse(
        detector
            .matches(ANY_PATH, Collections.singletonList(file("part-0.parquet", false)))
            .join());
  }

  @Test
  void doesNotMatchOnIcebergLayout() {
    assertFalse(
        detector
            .matches(ANY_PATH, Arrays.asList(file("metadata", true), file("data", true)))
            .join());
  }

  private static File file(String name, boolean dir) {
    return File.builder().filename(name).isDirectory(dir).lastModifiedAt(Instant.EPOCH).build();
  }
}
