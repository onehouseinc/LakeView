package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.ICEBERG_METADATA_FILE_SUFFIX;
import static ai.onehouse.constants.MetadataExtractorConstants.ICEBERG_METADATA_FOLDER_NAME;

import ai.onehouse.RuntimeModule.TableDiscoveryObjectStorageAsyncClient;
import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import com.google.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/**
 * A directory is an Iceberg table root if it contains a {@code metadata/} sub-directory <b>and</b>
 * that sub-directory contains at least one {@code *.metadata.json} pointer file. The pointer-file
 * check is what separates a real Iceberg table from any arbitrary folder that happens to have a
 * sub-directory named {@code metadata} (a Spark checkpoint dir, a documentation folder, custom
 * user layouts, etc.) — that name alone is too generic to be a reliable marker.
 *
 * <p>The validating LIST costs one extra storage call per candidate directory during discovery
 * (not per upload cycle). Discovery already short-circuits recursion on a match, so the extra
 * LIST runs at most once per real table and once per false-positive candidate during a discovery
 * pass.
 */
public class IcebergTableFormatDetector implements TableFormatDetector {
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;

  @Inject
  public IcebergTableFormatDetector(
      @Nonnull @TableDiscoveryObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
  }

  @Override
  public TableFormat format() {
    return TableFormat.ICEBERG;
  }

  @Override
  public CompletableFuture<Boolean> matches(String path, List<File> listedFiles) {
    // S3 ListObjectsV2 surfaces "directories" as CommonPrefixes, which the storage client
    // maps to File objects whose filename retains the trailing slash (e.g. "metadata/"),
    // since that is exactly the Prefix string S3 returns. Strip a trailing slash before
    // comparing so the check works regardless of whether the client preserves it.
    boolean hasMetadataDir =
        listedFiles.stream()
            .anyMatch(
                file -> {
                  if (!file.isDirectory()) {
                    return false;
                  }
                  String name = file.getFilename();
                  if (name == null) {
                    return false;
                  }
                  if (name.endsWith("/")) {
                    name = name.substring(0, name.length() - 1);
                  }
                  return ICEBERG_METADATA_FOLDER_NAME.equals(name);
                });
    if (!hasMetadataDir) {
      return CompletableFuture.completedFuture(false);
    }
    String metadataDirUri = storageUtils.constructFileUri(path, ICEBERG_METADATA_FOLDER_NAME);
    return asyncStorageClient
        .listAllFilesInDir(metadataDirUri)
        .thenApply(
            metadataFiles ->
                metadataFiles.stream()
                    .anyMatch(
                        f ->
                            !f.isDirectory()
                                && f.getFilename().endsWith(ICEBERG_METADATA_FILE_SUFFIX)));
  }
}
