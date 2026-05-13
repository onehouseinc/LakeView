package ai.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestIcebergTableFormatDetector {
  private static final String TABLE_PATH = "s3://bucket/db/t";
  private static final String METADATA_PATH = "s3://bucket/db/t/metadata";

  @Mock private AsyncStorageClient asyncStorageClient;
  private IcebergTableFormatDetector detector;

  @BeforeEach
  void setUp() {
    detector = new IcebergTableFormatDetector(asyncStorageClient, new StorageUtils());
  }

  @Test
  void declaresIcebergFormat() {
    assertEquals(TableFormat.ICEBERG, detector.format());
  }

  @Test
  void matchesWhenMetadataDirHasMetadataJson() {
    when(asyncStorageClient.listAllFilesInDir(eq(METADATA_PATH)))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(
                    file("v3.metadata.json", false),
                    file("snap-1.avro", false))));
    assertTrue(
        detector
            .matches(TABLE_PATH, Arrays.asList(file("metadata", true), file("data", true)))
            .join());
  }

  @Test
  void doesNotMatchWhenMetadataDirEmptyOfMetadataJson() {
    // Real false-positive: a folder happens to have a `metadata/` subdir but no Iceberg pointer.
    when(asyncStorageClient.listAllFilesInDir(eq(METADATA_PATH)))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(file("schema.csv", false), file("README.md", false))));
    assertFalse(
        detector
            .matches(TABLE_PATH, Arrays.asList(file("metadata", true), file("data", true)))
            .join());
  }

  @Test
  void doesNotMatchOnFileNamedMetadata() {
    // A non-directory entry named "metadata" must not be confused with the metadata/ folder, and
    // we must not issue the validating LIST when the marker isn't present.
    assertFalse(
        detector.matches(TABLE_PATH, Collections.singletonList(file("metadata", false))).join());
    verify(asyncStorageClient, never()).listAllFilesInDir(eq(METADATA_PATH));
  }

  @Test
  void doesNotMatchOnHudiLayout() {
    assertFalse(
        detector
            .matches(TABLE_PATH, Arrays.asList(file(".hoodie", true), file("part-0.parquet", false)))
            .join());
    verify(asyncStorageClient, never()).listAllFilesInDir(eq(METADATA_PATH));
  }

  private static File file(String name, boolean dir) {
    return File.builder().filename(name).isDirectory(dir).lastModifiedAt(Instant.EPOCH).build();
  }
}
