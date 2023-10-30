package com.onehouse.storage;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public interface AsyncStorageReader {
  CompletableFuture<InputStream> readFile(String path);
}
