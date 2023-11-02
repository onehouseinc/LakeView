package com.onehouse.metadataExtractor;

import static com.onehouse.metadataExtractor.Constants.HOODIE_FOLDER_NAME;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.configV1.ConfigV1;
import com.onehouse.config.configV1.Database;
import com.onehouse.config.configV1.MetadataExtractorConfig;
import com.onehouse.config.configV1.ParserConfig;
import com.onehouse.metadataExtractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;

public class TableDiscoveryService {
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;
  private final MetadataExtractorConfig metadataExtractorConfig;
  private final ExecutorService executorService;
  private final List<String> excludedPrefixes;

  @Inject
  public TableDiscoveryService(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull Config config,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
    this.metadataExtractorConfig = ((ConfigV1) config).getMetadataExtractorConfig();
    this.executorService = executorService;
    this.excludedPrefixes = metadataExtractorConfig.getPathsToExclude().orElse(List.of());
  }

  public CompletableFuture<Set<Table>> discoverTables() {
    List<Pair<String, CompletableFuture<Set<Table>>>> discoveredTablesFuture = new ArrayList<>();

    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        for (String basePath : database.getBasePaths()) {
          discoveredTablesFuture.add(
              Pair.of(
                  basePath,
                  discoverTablesInPath(basePath, parserConfig.getLake(), database.getName())));
        }
      }
    }

    return CompletableFuture.allOf(discoveredTablesFuture.toArray(new CompletableFuture[0]))
        .thenApply(
            ignored -> {
              Set<Table> allTablePaths = ConcurrentHashMap.newKeySet();
              for (Pair<String, CompletableFuture<Set<Table>>> pair : discoveredTablesFuture) {
                Set<Table> discoveredTables =
                    pair.getRight().join().stream()
                        .map(
                            table ->
                                table.toBuilder()
                                    .relativeTablePath(
                                        getRelativeTablePathFromUrl(
                                            pair.getLeft(), table.getAbsoluteTableUrl()))
                                    .build())
                        .collect(Collectors.toSet());
                allTablePaths.addAll(discoveredTables);
              }
              return allTablePaths;
            });
  }

  private CompletableFuture<Set<Table>> discoverTablesInPath(
      String path, String lakeName, String databaseName) {
    return asyncStorageClient
        .listFiles(path)
        .thenComposeAsync(
            listedFiles -> {
              Set<Table> tablePaths = ConcurrentHashMap.newKeySet();
              List<CompletableFuture<Void>> recursiveFutures = new ArrayList<>();

              if (isTableFolder(listedFiles)) {
                Table table =
                    Table.builder()
                        .absoluteTableUrl(path)
                        .databaseName(databaseName)
                        .lakeName(lakeName)
                        .build();
                if (!isExcluded(table.getAbsoluteTableUrl())) {
                  tablePaths.add(table);
                }
                return CompletableFuture.completedFuture(tablePaths);
              }

              for (File file : listedFiles) {
                if (file.getIsDirectory()) {
                  String filePath = storageUtils.constructFilePath(path, file.getFilename());
                  if (!isExcluded(filePath)) {
                    CompletableFuture<Void> recursiveFuture =
                        discoverTablesInPath(filePath, lakeName, databaseName)
                            .thenAccept(tablePaths::addAll);
                    recursiveFutures.add(recursiveFuture);
                  }
                }
              }

              return CompletableFuture.allOf(recursiveFutures.toArray(new CompletableFuture[0]))
                  .thenApplyAsync(ignored -> tablePaths, executorService);
            },
            executorService);
  }

  private static boolean isTableFolder(List<File> listedFiles) {
    return listedFiles.stream().anyMatch(file -> file.getFilename().startsWith(HOODIE_FOLDER_NAME));
  }

  private String getRelativeTablePathFromUrl(String baseStorageUrl, String tableAbsoluteUrl) {
    Path base = Paths.get(storageUtils.getPathFromUrl(baseStorageUrl));
    Path full = Paths.get(storageUtils.getPathFromUrl(tableAbsoluteUrl));
    Path relativePath = base.relativize(full);
    return relativePath.toString();
  }

  private boolean isExcluded(String filePath) {
    return excludedPrefixes.stream().anyMatch(filePath::startsWith);
  }
}
