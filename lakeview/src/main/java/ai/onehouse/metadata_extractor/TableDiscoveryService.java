package ai.onehouse.metadata_extractor;

import static ai.onehouse.metadata_extractor.MetadataExtractorUtils.getMetadataExtractorFailureReason;
import static java.util.Collections.emptySet;

import ai.onehouse.RuntimeModule.TableDiscoveryObjectStorageAsyncClient;
import ai.onehouse.api.models.request.TableFormat;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.config.models.configv1.TableHint;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * Discovers tables by walking all folders (including nested folders) in provided base paths.
 * Each {@link Database} in the parser YAML declares its {@code tableFormat}; this service picks
 * the single {@link TableFormatDetector} matching that format and applies it to every directory
 * under the database's base paths. Excluded paths are skipped.
 */
@Slf4j
public class TableDiscoveryService {
  private static final String TABLE_ID_SEPARATOR = "#";
  private final AsyncStorageClient asyncStorageClient;
  private final StorageUtils storageUtils;
  private final ExecutorService executorService;
  private final ConfigProvider configProvider;
  private final LakeViewExtractorMetrics lakeviewExtractorMetrics;
  private final Map<TableFormat, TableFormatDetector> detectorsByFormat;

  @Inject
  public TableDiscoveryService(
      @Nonnull @TableDiscoveryObjectStorageAsyncClient AsyncStorageClient asyncStorageClient,
      @Nonnull StorageUtils storageUtils,
      @Nonnull ConfigProvider configProvider,
      @Nonnull ExecutorService executorService,
      @Nonnull LakeViewExtractorMetrics lakeviewExtractorMetrics,
      @Nonnull HudiTableFormatDetector hudiTableFormatDetector,
      @Nonnull IcebergTableFormatDetector icebergTableFormatDetector) {
    this.asyncStorageClient = asyncStorageClient;
    this.storageUtils = storageUtils;
    this.executorService = executorService;
    this.configProvider = configProvider;
    this.lakeviewExtractorMetrics = lakeviewExtractorMetrics;
    this.detectorsByFormat =
        ImmutableMap.of(
            TableFormat.HUDI, hudiTableFormatDetector,
            TableFormat.ICEBERG, icebergTableFormatDetector);
  }

  public CompletableFuture<Set<Table>> discoverTables() {
    MetadataExtractorConfig metadataExtractorConfig =
        configProvider.getConfig().getMetadataExtractorConfig();
    List<String> excludedPathPatterns =
        metadataExtractorConfig.getPathExclusionPatterns().orElse(new ArrayList<>());
    log.info("Starting table discover service, excluding {}", excludedPathPatterns);
    List<Pair<String, CompletableFuture<Set<Table>>>> pathToDiscoveredTablesFuturePairList =
        new ArrayList<>();
    // Merge per-database tableHints into a single tableId -> hint map. Hints are optional metadata
    // supplied by the control plane (e.g. Iceberg metadata_location) and are looked up after
    // discovery, once the tableId is known.
    Map<String, TableHint> tableHintsByTableId = new HashMap<>();
    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        if (database.getTableHints() == null) {
          continue;
        }
        for (Map.Entry<String, TableHint> entry : database.getTableHints().entrySet()) {
          TableHint previous = tableHintsByTableId.put(entry.getKey(), entry.getValue());
          if (previous != null) {
            log.warn(
                "Duplicate tableHint for tableId {} across databases; later entry wins.",
                entry.getKey());
          }
        }
      }
    }

    for (ParserConfig parserConfig : metadataExtractorConfig.getParserConfig()) {
      for (Database database : parserConfig.getDatabases()) {
        TableFormatDetector detector = detectorFor(database);
        for (String basePathConfig : database.getBasePaths()) {
          String basePath = extractBasePath(basePathConfig);

          if (isExcluded(basePath, excludedPathPatterns)) {
            log.warn("Provided base-path has also been passed under paths to exclude {}", basePath);
          }

          pathToDiscoveredTablesFuturePairList.add(
              Pair.of(
                  basePathConfig,
                  discoverTablesInPath(
                      basePath,
                      parserConfig.getLake(),
                      database.getName(),
                      excludedPathPatterns,
                      detector)));
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
                    log.debug(
                        String.format(
                            "For tableId %s, there must be exactly one table in path %s",
                            tableId, extractBasePath(basePathConfig)));
                    continue;
                  }
                  Table table = discoveredTables.iterator().next();
                  Table.TableBuilder builder = table.toBuilder().tableId(tableId);
                  TableHint hint = tableHintsByTableId.get(tableId);
                  if (hint != null && StringUtils.isNotBlank(hint.getMetadataLocationHint())) {
                    builder.metadataLocationHint(hint.getMetadataLocationHint());
                  }
                  table = builder.build();
                  discoveredTables = Collections.singleton(table);
                }

                allTablePaths.addAll(discoveredTables);
              }
              return allTablePaths;
            });
  }

  private TableFormatDetector detectorFor(Database database) {
    TableFormat declared = database.getTableFormat() == null ? TableFormat.HUDI : database.getTableFormat();
    TableFormatDetector detector = detectorsByFormat.get(declared);
    if (detector == null) {
      // Programmer error: a new TableFormat enum value was added without a matching detector.
      throw new IllegalStateException("No detector registered for tableFormat=" + declared);
    }
    return detector;
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
      String path,
      String lakeName,
      String databaseName,
      List<String> excludedPathPatterns,
      TableFormatDetector detector) {
    try {
      log.info(String.format("Discovering tables in %s", path));
      return asyncStorageClient
          .listAllFilesInDir(path)
          .thenComposeAsync(
              listedFiles ->
                  detector
                      .matches(path, listedFiles)
                      .thenComposeAsync(
                          matched -> {
                            Set<Table> tablePaths = ConcurrentHashMap.newKeySet();
                            if (Boolean.TRUE.equals(matched)) {
                              Table table =
                                  Table.builder()
                                      .absoluteTableUri(path)
                                      .databaseName(databaseName)
                                      .lakeName(lakeName)
                                      .tableFormat(detector.format())
                                      .build();
                              if (!isExcluded(table.getAbsoluteTableUri(), excludedPathPatterns)) {
                                tablePaths.add(table);
                              }
                              return CompletableFuture.completedFuture(tablePaths);
                            }

                            List<File> directories =
                                listedFiles.stream()
                                    .filter(File::isDirectory)
                                    .collect(Collectors.toList());
                            List<CompletableFuture<Void>> recursiveFutures = new ArrayList<>();
                            for (File file : directories) {
                              String filePath =
                                  storageUtils.constructFileUri(path, file.getFilename());
                              if (!isExcluded(filePath, excludedPathPatterns)) {
                                CompletableFuture<Void> recursiveFuture =
                                    discoverTablesInPath(
                                            filePath,
                                            lakeName,
                                            databaseName,
                                            excludedPathPatterns,
                                            detector)
                                        .thenAccept(tablePaths::addAll);
                                recursiveFutures.add(recursiveFuture);
                              }
                            }
                            return CompletableFuture.allOf(
                                    recursiveFutures.toArray(new CompletableFuture[0]))
                                .thenApplyAsync(ignored -> tablePaths, executorService);
                          },
                          executorService),
              executorService)
          .exceptionally(
              e -> {
                log.error("Failed to discover tables in path: {}", path, e);
                lakeviewExtractorMetrics.incrementTableDiscoveryFailureCounter(
                    getMetadataExtractorFailureReason(
                        e, MetricsConstants.MetadataUploadFailureReasons.UNKNOWN));
                return emptySet();
              });
    } catch (Exception e) {
      log.error("Failed to discover tables in path: {}", path, e);
      return CompletableFuture.completedFuture(emptySet());
    }
  }

  private boolean isExcluded(String filePath, List<String> excludedPathPatterns) {
    return excludedPathPatterns.stream().anyMatch(filePath::matches);
  }
}
