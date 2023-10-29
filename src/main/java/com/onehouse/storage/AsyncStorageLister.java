package com.onehouse.storage;

import com.onehouse.storage.models.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncStorageLister {
  CompletableFuture<List<File>> listFiles(String path);
}
