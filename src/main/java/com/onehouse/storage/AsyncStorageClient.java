package com.onehouse.storage;

import com.onehouse.storage.models.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncStorageClient {
  CompletableFuture<List<File>> listFiles(String path);

  CompletableFuture<InputStream> readFileAsInputStream(String path);

  CompletableFuture<byte[]> readFileAsBytes(String path);
}
