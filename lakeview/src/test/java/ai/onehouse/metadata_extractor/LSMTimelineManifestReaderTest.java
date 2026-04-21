package ai.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import ai.onehouse.metadata_extractor.LSMTimelineManifestReader.ManifestSnapshot;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LSMTimelineManifestReaderTest {
  @Mock private AsyncStorageClient asyncStorageClient;
  private LSMTimelineManifestReader reader;

  private static final String HISTORY_URI = "s3://bucket/table/.hoodie/timeline/history/";

  @BeforeEach
  void setUp() {
    reader = new LSMTimelineManifestReader(asyncStorageClient, new StorageUtils());
  }

  @Test
  void testReadLatestManifest() {
    mockReadFile(HISTORY_URI + "_version_", "3");
    mockReadFile(
        HISTORY_URI + "manifest_3",
        "{\n"
            + "  \"files\" : [ {\n"
            + "    \"fileName\" : \"20260101_20260102_0.parquet\",\n"
            + "    \"fileLen\" : 1024\n"
            + "  }, {\n"
            + "    \"fileName\" : \"20260102_20260103_1.parquet\",\n"
            + "    \"fileLen\" : 2048\n"
            + "  } ]\n"
            + "}");

    ManifestSnapshot snapshot = reader.readLatestManifest(HISTORY_URI).join();

    assertEquals(3, snapshot.getVersion());
    assertEquals(
        Arrays.asList("20260101_20260102_0.parquet", "20260102_20260103_1.parquet"),
        snapshot.getParquetFileNames());
    assertTrue(!snapshot.isEmpty());
  }

  @Test
  void testReadLatestManifest_VersionFileMissing() {
    when(asyncStorageClient.readFileAsBytes(HISTORY_URI + "_version_"))
        .thenReturn(failedFuture(new RuntimeException("NoSuchKey")));

    ManifestSnapshot snapshot = reader.readLatestManifest(HISTORY_URI).join();

    assertTrue(snapshot.isEmpty());
    assertEquals(0, snapshot.getVersion());
    assertEquals(Collections.emptyList(), snapshot.getParquetFileNames());
  }

  @Test
  void testReadLatestManifest_EmptyFilesArray() {
    mockReadFile(HISTORY_URI + "_version_", "1");
    mockReadFile(HISTORY_URI + "manifest_1", "{ \"files\" : [] }");

    ManifestSnapshot snapshot = reader.readLatestManifest(HISTORY_URI).join();

    assertEquals(1, snapshot.getVersion());
    assertEquals(Collections.emptyList(), snapshot.getParquetFileNames());
    assertTrue(!snapshot.isEmpty());
  }

  @Test
  void testReadManifestFileNames() {
    mockReadFile(
        HISTORY_URI + "manifest_5",
        "{ \"files\" : [ { \"fileName\" : \"a.parquet\", \"fileLen\" : 100 } ] }");

    List<String> files = reader.readManifestFileNames(HISTORY_URI, 5).join();

    assertEquals(Collections.singletonList("a.parquet"), files);
  }

  @Test
  void testReadManifestFileNames_ManifestMissing() {
    when(asyncStorageClient.readFileAsBytes(HISTORY_URI + "manifest_99"))
        .thenReturn(failedFuture(new RuntimeException("NoSuchKey")));

    List<String> files = reader.readManifestFileNames(HISTORY_URI, 99).join();

    assertEquals(Collections.emptyList(), files);
  }

  @Test
  void testReadLatestManifest_IgnoresUnknownFields() {
    mockReadFile(HISTORY_URI + "_version_", "1");
    mockReadFile(
        HISTORY_URI + "manifest_1",
        "{ \"files\" : [ { \"fileName\" : \"x.parquet\", \"fileLen\" : 42,"
            + " \"unknownField\" : true } ], \"extraTopLevel\" : 123 }");

    ManifestSnapshot snapshot = reader.readLatestManifest(HISTORY_URI).join();

    assertEquals(1, snapshot.getVersion());
    assertEquals(Collections.singletonList("x.parquet"), snapshot.getParquetFileNames());
  }

  @Test
  void testReadLatestManifest_VersionWithWhitespace() {
    mockReadFile(HISTORY_URI + "_version_", "  7\n");
    mockReadFile(HISTORY_URI + "manifest_7", "{ \"files\" : [] }");

    ManifestSnapshot snapshot = reader.readLatestManifest(HISTORY_URI).join();

    assertEquals(7, snapshot.getVersion());
  }

  @Test
  void testManifestSnapshotEmpty() {
    ManifestSnapshot empty = ManifestSnapshot.empty();
    assertTrue(empty.isEmpty());
    assertEquals(0, empty.getVersion());
    assertEquals(Collections.emptyList(), empty.getParquetFileNames());
  }

  private void mockReadFile(String uri, String content) {
    when(asyncStorageClient.readFileAsBytes(uri))
        .thenReturn(
            CompletableFuture.completedFuture(content.getBytes(StandardCharsets.UTF_8)));
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable ex) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(ex);
    return f;
  }
}
