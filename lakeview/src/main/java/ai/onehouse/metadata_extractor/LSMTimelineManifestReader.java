package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.MANIFEST_FILE_PREFIX;
import static ai.onehouse.constants.MetadataExtractorConstants.VERSION_MARKER_FILE;

import ai.onehouse.RuntimeModule.TableMetadataUploadObjectStorageAsyncClient;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads the Hudi LSM (V2) archived timeline manifest layout that lives under {@code
 * .hoodie/timeline/history/}. The layout, written by Hudi 1.x, is:
 *
 * <pre>
 *   _version_                                # text file containing the latest manifest version
 *   manifest_N                               # JSON: { "files": [{ "fileName": "...", "fileLen": N }] }
 *   {minInstant}_{maxInstant}_{level}.parquet
 * </pre>
 *
 * <p>The manifest is the source of truth for which parquet files are valid in the current
 * snapshot. Compaction rewrites multiple low-level files into a single higher-level file, leaving
 * orphaned data files on storage; only files referenced by the latest manifest should be treated
 * as live. LakeView mirrors files to the backend rather than parsing them, so it only needs the
 * filenames the manifest lists - no Hudi dependency required.
 */
@Slf4j
public class LSMTimelineManifestReader {
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;
  private final ObjectMapper mapper = new ObjectMapper();

  @Inject
  public LSMTimelineManifestReader(
      @Nonnull @TableMetadataUploadObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
  }

  /**
   * Reads {@code _version_} and the corresponding {@code manifest_N} from the given history
   * directory. Returns {@link ManifestSnapshot#empty()} when the {@code _version_} file does not
   * exist (no archives have been written yet).
   */
  public CompletableFuture<ManifestSnapshot> readLatestManifest(String historyDirectoryUri) {
    String versionFileUri = storageUtils.constructFileUri(historyDirectoryUri, VERSION_MARKER_FILE);
    return asyncStorageClient
        .readFileAsBytes(versionFileUri)
        .thenCompose(
            versionBytes -> {
              int version = parseVersionFile(versionBytes);
              return readManifestForVersion(historyDirectoryUri, version)
                  .thenApply(files -> ManifestSnapshot.of(version, files));
            })
        .exceptionally(
            throwable -> {
              log.info(
                  "No V2 archived timeline _version_ file at {} (treating as empty). Reason: {}",
                  versionFileUri,
                  throwable.getMessage());
              return ManifestSnapshot.empty();
            });
  }

  /**
   * Reads {@code manifest_N} for the given version and returns the parquet filenames it
   * references. Returns an empty list if the manifest cannot be read - callers treat this as
   * "previous snapshot is gone, fall back to bootstrap" rather than a hard failure.
   */
  public CompletableFuture<List<String>> readManifestFileNames(
      String historyDirectoryUri, int version) {
    return readManifestForVersion(historyDirectoryUri, version)
        .exceptionally(
            throwable -> {
              log.warn(
                  "Failed to read manifest_{} from {}; treating as missing. Reason: {}",
                  version,
                  historyDirectoryUri,
                  throwable.getMessage());
              return Collections.emptyList();
            });
  }

  private CompletableFuture<List<String>> readManifestForVersion(
      String historyDirectoryUri, int version) {
    String manifestUri =
        storageUtils.constructFileUri(historyDirectoryUri, MANIFEST_FILE_PREFIX + version);
    return asyncStorageClient
        .readFileAsBytes(manifestUri)
        .thenApply(
            bytes -> {
              try {
                return parseManifestFileNames(bytes);
              } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to parse LSM manifest at " + manifestUri, e);
              }
            });
  }

  private static int parseVersionFile(byte[] bytes) {
    String contents = new String(bytes, StandardCharsets.UTF_8).trim();
    return Integer.parseInt(contents);
  }

  private List<String> parseManifestFileNames(byte[] bytes) throws IOException {
    JsonNode root = mapper.readTree(bytes);
    JsonNode files = root.get("files");
    List<String> result = new ArrayList<>();
    if (files != null && files.isArray()) {
      for (JsonNode entry : files) {
        JsonNode name = entry.get("fileName");
        if (name != null && !name.asText().isEmpty()) {
          result.add(name.asText());
        }
      }
    }
    return result;
  }

  /** Snapshot of the latest LSM manifest: version number plus the parquet filenames it lists. */
  @Value(staticConstructor = "of")
  public static class ManifestSnapshot {
    int version;
    @Nonnull List<String> parquetFileNames;

    public static ManifestSnapshot empty() {
      return ManifestSnapshot.of(0, Collections.emptyList());
    }

    public boolean isEmpty() {
      return version == 0;
    }
  }
}
