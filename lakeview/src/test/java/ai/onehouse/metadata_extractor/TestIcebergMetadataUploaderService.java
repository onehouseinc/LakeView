package ai.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.onehouse.storage.models.File;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TestIcebergMetadataUploaderService {

  @Test
  void picksLatestHiveGlueStyleMetadataJsonByNumericPrefix() {
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                jsonFile("00000-abc.metadata.json"),
                jsonFile("00020-xyz.metadata.json"),
                jsonFile("00005-def.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("00020-xyz.metadata.json", latest.get().getFilename());
  }

  @Test
  void picksLatestHadoopCatalogMetadataJsonByVersionNumber() {
    // v10 is numerically newer than v2 — lex-sort would pick v2, which would be wrong. Numeric
    // comparison on the leading integer correctly returns v10.
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                jsonFile("v1.metadata.json"),
                jsonFile("v2.metadata.json"),
                jsonFile("v9.metadata.json"),
                jsonFile("v10.metadata.json"),
                jsonFile("v11.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("v11.metadata.json", latest.get().getFilename());
  }

  @Test
  void ignoresDirectoriesAndUnrelatedFiles() {
    Optional<File> latest =
        IcebergMetadataUploaderService.pickLatestMetadataJson(
            Arrays.asList(
                File.builder().filename("snap-x.avro").isDirectory(false).lastModifiedAt(Instant.EPOCH).build(),
                File.builder().filename("sub").isDirectory(true).lastModifiedAt(Instant.EPOCH).build(),
                jsonFile("00001-a.metadata.json")));
    assertTrue(latest.isPresent());
    assertEquals("00001-a.metadata.json", latest.get().getFilename());
  }

  @Test
  void returnsEmptyWhenNoMetadataJson() {
    assertFalse(
        IcebergMetadataUploaderService.pickLatestMetadataJson(
                Collections.singletonList(
                    File.builder().filename("snap.avro").isDirectory(false).lastModifiedAt(Instant.EPOCH).build()))
            .isPresent());
  }

  @Test
  void extractVersionNumberHandlesAllNamingShapes() {
    assertEquals(10L, IcebergMetadataUploaderService.extractVersionNumber("v10.metadata.json"));
    assertEquals(20L, IcebergMetadataUploaderService.extractVersionNumber("00020-uuid.metadata.json"));
    assertEquals(0L, IcebergMetadataUploaderService.extractVersionNumber("00000-uuid.metadata.json"));
    // Non-numeric filenames still parse to -1 so they don't dominate the sort.
    assertEquals(-1L, IcebergMetadataUploaderService.extractVersionNumber("metadata.json"));
    assertEquals(-1L, IcebergMetadataUploaderService.extractVersionNumber("v.metadata.json"));
  }

  @Test
  void resolveFromVersionHintReturnsTargetWhenPresent() {
    Optional<File> resolved =
        IcebergMetadataUploaderService.resolveFromVersionHint(
            "7\n".getBytes(StandardCharsets.UTF_8),
            Arrays.asList(
                jsonFile("v6.metadata.json"),
                jsonFile("v7.metadata.json"),
                jsonFile("v8.metadata.json")));
    assertTrue(resolved.isPresent());
    assertEquals("v7.metadata.json", resolved.get().getFilename());
  }

  @Test
  void resolveFromVersionHintReturnsEmptyWhenTargetMissing() {
    // Hint points at v42 but only v6 / v7 exist — caller will fall back to numeric sort.
    assertFalse(
        IcebergMetadataUploaderService.resolveFromVersionHint(
                "42".getBytes(StandardCharsets.UTF_8),
                Arrays.asList(jsonFile("v6.metadata.json"), jsonFile("v7.metadata.json")))
            .isPresent());
  }

  @Test
  void resolveFromVersionHintReturnsEmptyOnNonIntegerContent() {
    assertFalse(
        IcebergMetadataUploaderService.resolveFromVersionHint(
                "not-an-int".getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(jsonFile("v1.metadata.json")))
            .isPresent());
  }

  @Test
  void lastPathSegmentExtractsFilenameFromUri() {
    assertEquals(
        "00042-uuid.metadata.json",
        IcebergMetadataUploaderService.lastPathSegment(
            "s3://bucket/db/t/metadata/00042-uuid.metadata.json"));
    assertEquals(
        "v3.metadata.json",
        IcebergMetadataUploaderService.lastPathSegment(
            "s3a://bucket/db/t/metadata/v3.metadata.json"));
  }

  @Test
  void lastPathSegmentReturnsInputWhenNoSlash() {
    assertEquals("filename.json", IcebergMetadataUploaderService.lastPathSegment("filename.json"));
  }

  private static File jsonFile(String name) {
    return File.builder().filename(name).isDirectory(false).lastModifiedAt(Instant.EPOCH).build();
  }
}
