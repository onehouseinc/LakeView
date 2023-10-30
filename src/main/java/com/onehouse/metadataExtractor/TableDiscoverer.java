package com.onehouse.metadataExtractor;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.configV1.ConfigV1;
import com.onehouse.config.configV1.Database;
import com.onehouse.config.configV1.MetadataExtractorConfig;
import com.onehouse.config.configV1.ParserConfig;
import com.onehouse.metadataExtractor.models.Table;
import com.onehouse.storage.AsyncStorageLister;
import com.onehouse.storage.models.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

public class TableDiscoverer {
  private final AsyncStorageLister asyncStorageLister;
  private final MetadataExtractorConfig metadataExtractorConfig;
  private final ExecutorService executorService;
  private static final String HOODIE_FOLDER_NAME = ".Hoodie";

  @Inject
  public TableDiscoverer(
      @Nonnull AsyncStorageLister asyncStorageLister,
      @Nonnull Config config,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageLister = asyncStorageLister;
    this.metadataExtractorConfig = ((ConfigV1) config).getMetadataExtractorConfig();
    this.executorService = executorService;
  }

  public CompletableFuture<Set<Table>> discoverTables() {
    List<CompletableFuture<Set<Table>>> discoveredTablesFuture = new ArrayList<>();

    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        for (String basePath : database.getBasePaths()) {
          discoveredTablesFuture.add(
              discoverTablesInPath(basePath, parserConfig.getLake(), database.getName()));
        }
      }
    }

    return CompletableFuture.allOf(discoveredTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(
            ignored -> {
              Set<Table> allTablePaths = ConcurrentHashMap.newKeySet();
              for (CompletableFuture<Set<Table>> future : discoveredTablesFuture) {
                allTablePaths.addAll(future.join());
              }
              return allTablePaths;
            });
  }

  private CompletableFuture<Set<Table>> discoverTablesInPath(
      String path, String lakeName, String databaseName) {
    return asyncStorageLister
        .listFiles(path)
        .thenComposeAsync(
            listedFiles -> {
              Set<Table> tablePaths = ConcurrentHashMap.newKeySet();
              List<CompletableFuture<Void>> recursiveFutures = new ArrayList<>();

              if (isTableFolder(listedFiles)) {
                tablePaths.add(
                    Table.builder()
                        .tablePath(path)
                        .databaseName(databaseName)
                        .lakeName(lakeName)
                        .build());
                return CompletableFuture.completedFuture(tablePaths);
              }

              for (File file : listedFiles) {
                if (file.getIsDirectory()) {
                  String filePath = getNestedFilePath(path, file);
                  CompletableFuture<Void> recursiveFuture =
                      discoverTablesInPath(filePath, lakeName, databaseName)
                          .thenAccept(tablePaths::addAll);
                  recursiveFutures.add(recursiveFuture);
                }
              }

              return CompletableFuture.allOf(recursiveFutures.toArray(new CompletableFuture[0]))
                  .thenApplyAsync(ignored -> tablePaths, executorService);
            },
            executorService);
  }

  private static boolean isTableFolder(List<File> listedFiles) {
    return listedFiles.stream().anyMatch(file -> file.getFilename().equals(HOODIE_FOLDER_NAME));
  }

  private static String getNestedFilePath(String basePath, File file) {
    return String.format(
        "%s/%s",
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath,
        file.getFilename());
  }
}
