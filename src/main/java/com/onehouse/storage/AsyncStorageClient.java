package com.onehouse.storage;

import com.onehouse.storage.models.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

public interface AsyncStorageClient {
  CompletableFuture<List<File>> listAllFilesInDir(String path);

  CompletableFuture<InputStream> readFileAsInputStream(String path);

  CompletableFuture<byte[]> readFileAsBytes(String path);

  CompletableFuture<Pair<String, List<File>>> fetchObjectsByPage(
      String bucketName, String prefix, String continuationToken);
}
