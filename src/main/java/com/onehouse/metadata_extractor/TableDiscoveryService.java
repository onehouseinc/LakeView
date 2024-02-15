package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;

import com.google.inject.Inject;
import com.onehouse.config.ConfigProvider;
import com.onehouse.config.models.configv1.Database;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
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
  private final ExecutorService executorService;
  private final ConfigProvider configProvider;

  @Inject
  public TableDiscoveryService(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ConfigProvider configProvider,
      @Nonnull ExecutorService executorService) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.configProvider = configProvider;
  }

  public CompletableFuture<Set<Table>> discoverTables() {
    MetadataExtractorConfig metadataExtractorConfig =
        configProvider.getConfig().getMetadataExtractorConfig();
    List<String> excludedPathPatterns =
        metadataExtractorConfig.getPathExclusionPatterns().orElse(new ArrayList<>());
    log.info("Starting table discover service, excluding {}", excludedPathPatterns);
    List<Pair<String, CompletableFuture<Set<Table>>>> pathToDiscoveredTablesFuturePairList =
        new ArrayList<>();

    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        for (String basePath : database.getBasePaths()) {

          if (isExcluded(basePath, excludedPathPatterns)) {
            throw new IllegalArgumentException(
                "Provided base path cannot be part of paths to excluded");
          }

          pathToDiscoveredTablesFuturePairList.add(
              Pair.of(
                  basePath,
                  discoverTablesInPath(
                      basePath, parserConfig.getLake(), database.getName(), excludedPathPatterns)));
        }
      }
    }

    return CompletableFuture.allOf(
            pathToDiscoveredTablesFuturePairList.stream()
                .map(Pair::getRight)
                .toArray(CompletableFuture[]::new))
        .thenApply(
            ignored -> {
              Set<Table> allTablePaths = ConcurrentHashMap.newKeySet();
              for (Pair<String, CompletableFuture<Set<Table>>> pathToDiscoveredTablesPair :
                  pathToDiscoveredTablesFuturePairList) {
                allTablePaths.addAll(pathToDiscoveredTablesPair.getRight().join());
              }
              return allTablePaths;
            });
  }

  private CompletableFuture<Set<Table>> discoverTablesInPath(
      String path, String lakeName, String databaseName, List<String> excludedPathPatterns) {
    log.info(String.format("Discovering tables in %s", path));
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
                if (!isExcluded(table.getAbsoluteTableUri(), excludedPathPatterns)) {
                  tablePaths.add(table);
                }
                return CompletableFuture.completedFuture(tablePaths);
              }

              List<File> directories =
                  listedFiles.stream().filter(File::isDirectory).collect(Collectors.toList());

              for (File file : directories) {
                String filePath = storageUtils.constructFileUri(path, file.getFilename());
                if (!isExcluded(filePath, excludedPathPatterns)) {
                  CompletableFuture<Void> recursiveFuture =
                      discoverTablesInPath(filePath, lakeName, databaseName, excludedPathPatterns)
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

  private boolean isExcluded(String filePath, List<String> excludedPathPatterns) {
    return excludedPathPatterns.stream().anyMatch(filePath::matches);
  }
}
