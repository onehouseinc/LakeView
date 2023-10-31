package com.onehouse.metadataExtractor;

import static com.onehouse.metadataExtractor.Constants.HOODIE_FOLDER_NAME;
import static com.onehouse.metadataExtractor.Constants.HOODIE_PROPERTIES_FILE;

import com.google.inject.Inject;
import com.onehouse.api.OnehouseApiClient;
import com.onehouse.metadataExtractor.models.Table;
import com.onehouse.storage.AsyncStorageLister;
import com.onehouse.storage.S3AsyncFileUploader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

public class TableProcessorService {
  private final AsyncStorageLister asyncStorageLister;
  private final HoodiePropertiesReader hoodiePropertiesReader;
  private final S3AsyncFileUploader s3AsyncFileUploader;
  private final OnehouseApiClient onehouseApiClient;
  private final ExecutorService executorService;

  @Inject
  public TableProcessorService(
      @Nonnull AsyncStorageLister asyncStorageLister,
      @Nonnull HoodiePropertiesReader hoodiePropertiesReader,
      @Nonnull S3AsyncFileUploader s3AsyncFileUploader,
      @Nonnull OnehouseApiClient onehouseApiClient,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageLister = asyncStorageLister;
    this.hoodiePropertiesReader = hoodiePropertiesReader;
    this.s3AsyncFileUploader = s3AsyncFileUploader;
    this.onehouseApiClient = onehouseApiClient;
    this.executorService = executorService;
  }

  public CompletableFuture<Void> processTables(Set<Table> tablesToProcess) {
    List<CompletableFuture<Void>> processTablesFuture = new ArrayList<>();
    for (Table table : tablesToProcess) {
      processTablesFuture.add(processTable(table));
    }

    return CompletableFuture.allOf(processTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(ignored -> null);
  }

  private CompletableFuture<Void> processTable(Table table) {
    // Read hoodie.properties file
    return CompletableFuture.completedFuture(null);
  }

  private String getHoodiePropertiesFilePath(Table table) {
    String basePath = table.getTablePath();
    return String.format(
        "%s/%s/%s",
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath,
        HOODIE_FOLDER_NAME,
        HOODIE_PROPERTIES_FILE);
  }
}
