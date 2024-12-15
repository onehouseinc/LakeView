package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.HOODIE_FOLDER_NAME;
import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;
import static java.util.Collections.emptySet;

import com.google.inject.Inject;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/*
 * Discovers hudi tables by Parsing all folders (including nested folders) in provided base paths
 * excluded paths will be skipped.
 */
@Slf4j
public class TableDiscoveryService {
  private static final String TABLE_ID_SEPARATOR = "#";
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;
  private final ConfigProvider configProvider;
  private final LakeViewExtractorMetrics hudiMetadataExtractorMetrics;

  @Inject
  public TableDiscoveryService(
      @Nonnull AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ConfigProvider configProvider,
      @Nonnull ExecutorService executorService,
      @Nonnull LakeViewExtractorMetrics hudiMetadataExtractorMetrics) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.configProvider = configProvider;
    this.hudiMetadataExtractorMetrics = hudiMetadataExtractorMetrics;
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
        for (String basePathConfig : database.getBasePaths()) {
          String basePath = extractBasePath(basePathConfig);

          if (isExcluded(basePath, excludedPathPatterns)) {
            log.warn("Provided base-path has also been passed under paths to exclude {}", basePath);
          }

          pathToDiscoveredTablesFuturePairList.add(
              Pair.of(
                  basePathConfig,
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

                Set<Table> discoveredTables = pathToDiscoveredTablesPair.getRight().join();

                String basePathConfig = pathToDiscoveredTablesPair.getLeft();
                String tableId = extractTableId(basePathConfig);
                if (StringUtils.isNotBlank(tableId)) {
                  if (discoveredTables.size() != 1) {
                    log.error(
                        String.format(
                            "For tableId %s, there must be exactly one table in path %s",
                            tableId, extractBasePath(basePathConfig)));
                    continue;
                  }
                  Table table = discoveredTables.iterator().next();
                  table = table.toBuilder().tableId(tableId).build();
                  discoveredTables = Collections.singleton(table);
                }

                allTablePaths.addAll(discoveredTables);
              }
              return allTablePaths;
            });
  }

  private String extractBasePath(String basePathConfig) {
    String[] basePathConfigParts = basePathConfig.split(TABLE_ID_SEPARATOR);
    return basePathConfigParts[0];
  }

  private String extractTableId(String basePathConfig) {
    String[] basePathConfigParts = basePathConfig.split(TABLE_ID_SEPARATOR);
    return basePathConfigParts.length > 1 ? basePathConfigParts[1] : "";
  }

  private CompletableFuture<Set<Table>> discoverTablesInPath(
      String path, String lakeName, String databaseName, List<String> excludedPathPatterns) {
    try {
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
              executorService)
          .exceptionally(
              e -> {
                log.error("Failed to discover tables in path: {}", path);
                log.error(e.getMessage(), e);
                hudiMetadataExtractorMetrics.incrementTableDiscoveryFailureCounter(
                  getMetadataExtractorFailureReason(
                    e,
                    MetricsConstants.MetadataUploadFailureReasons.UNKNOWN)
                );
                return emptySet();
              });
    } catch (Exception e) {
      log.error("Failed to discover tables in path: {}", path);
      log.error(e.getMessage(), e);
      return CompletableFuture.completedFuture(emptySet());
    }
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
