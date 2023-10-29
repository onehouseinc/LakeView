package com.onehouse.storage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncStorageLister {
  CompletableFuture<List<String>> listFiles(String path);
}
