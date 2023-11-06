package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;

import com.google.inject.Inject;
import com.onehouse.config.Config;
import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.config.models.configv1.Database;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import com.onehouse.metadata_extractor.models.Table;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/*
 * Discovers hudi tables by Parsing all folders (including nested folders) in provided base paths
 * excluded paths will be skipped.
 */
@Slf4j
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
    log.debug(String.format("Starting table discover service, excluding %s", excludedPrefixes));
    List<Pair<String, CompletableFuture<Set<Table>>>> discoveredTablesFuture = new ArrayList<>();

    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        for (String basePath : database.getBasePaths()) {

          if (isExcluded(basePath)) {
            throw new IllegalArgumentException(
                "Provided base path cannot be part of paths to excluded");
          }

          discoveredTablesFuture.add(
              Pair.of(
                  basePath,
                  discoverTablesInPath(basePath, parserConfig.getLake(), database.getName())));
        }
      }
    }

    return CompletableFuture.allOf(
            discoveredTablesFuture.stream()
                .map(Pair::getRight)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
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
                                            pair.getLeft(), table.getAbsoluteTableUri()))
                                    .build())
                        .collect(Collectors.toSet());
                allTablePaths.addAll(discoveredTables);
              }
              return allTablePaths;
            });
  }

  private CompletableFuture<Set<Table>> discoverTablesInPath(
      String path, String lakeName, String databaseName) {
    log.debug(String.format("Discovering tables in %s", path));
    return asyncStorageClient
        .listAllFilesInDir(path)
        .thenComposeAsync(
            listedFiles -> {
              Set<Table> tablePaths = ConcurrentHashMap.newKeySet();
              List<CompletableFuture<Void>> recursiveFutures = new ArrayList<>();

              if (isHudiTableFolder(listedFiles)) {
                Table table =
                    Table.builder()
                        .absoluteTableUri(path)
                        .databaseName(databaseName)
                        .lakeName(lakeName)
                        .build();
                if (!isExcluded(table.getAbsoluteTableUri())) {
                  tablePaths.add(table);
                }
                return CompletableFuture.completedFuture(tablePaths);
              }

              List<File> directories =
                  listedFiles.stream().filter(File::isDirectory).collect(Collectors.toList());

              for (File file : directories) {
                String filePath = storageUtils.constructFileUri(path, file.getFilename());
                if (!isExcluded(filePath)) {
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

  /*
   *  checks the contents of a folder to see if it is a hudi table or not
   *  a folder is a hudi table if it contains .hoodie folder within it
   */
  private static boolean isHudiTableFolder(List<File> listedFiles) {
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
