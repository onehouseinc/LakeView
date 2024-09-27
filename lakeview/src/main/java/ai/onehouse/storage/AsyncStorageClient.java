package ai.onehouse.storage;

import ai.onehouse.storage.models.File;
import ai.onehouse.storage.models.FileStreamData;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

public interface AsyncStorageClient {
  CompletableFuture<List<File>> listAllFilesInDir(String path);

  CompletableFuture<FileStreamData> streamFileAsync(String path);

  CompletableFuture<byte[]> readFileAsBytes(String path);

  CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String bucketName, String prefix, String continuationToken, String startAfter);
}
