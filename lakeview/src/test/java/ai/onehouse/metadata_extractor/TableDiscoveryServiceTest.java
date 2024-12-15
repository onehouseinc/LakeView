package ai.onehouse.metadata_extractor;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.configv1.ConfigV1;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.models.File;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableDiscoveryServiceTest {

  private TableDiscoveryService tableDiscoveryService;

  @Mock private AsyncStorageClient asyncStorageClient;
  @Mock private ConfigV1 config;
  @Mock private MetadataExtractorConfig metadataExtractorConfig;
  @Mock private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;

  private static final String BASE_PATH = "s3://bucket/base_path/";
  private static final String BASE_PATH_2 = "s3://bucket/base_path_2/";

  private static final String LAKE = "lake";
  private static final String DATABASE = "database";

  @Test
  void testDiscoverTablesWithExclusion() throws ExecutionException, InterruptedException {
    /*
     * Mock the asyncStorageClient to simulate the file system structure for testing the discovery of tables.
     * The simulated folder structure is as follows:
     *
     * s3://bucket/base_path
     * │
     * ├─ /file1 (regular file, not a table)
     * │
     * ├─ /table1 (Hudi table directory)
     * │   └─ /.hoodie (presence of this folder indicates a Hudi table)
     * │
     * ├─ /excluded (directory that should be excluded from table discovery)
     * │   ├─ /excluded-table-1 (Hudi table, but should be excluded)
     * │   │   └─ /.hoodie
     * │   └─ /excluded-table-2 (Hudi table, but should be excluded)
     * │       └─ /.hoodie
     * │
     * └─ /nested-folder (directory containing nested tables and folders)
     *     ├─ /table2 (nested Hudi table directory)
     *     │   └─ /.hoodie
     *     ├─ /excluded-table-3 (nested Hudi table, but should be excluded)
     *     │   └─ /.hoodie
     *     └─ /unrelated-folder1 (a regular folder, not a table)
     *
     * s3://bucket/base_path_2
     * │
     * ├─ /tableWithId (Hudi table directory)
     * │   └─ /.hoodie (presence of this folder indicates a Hudi table)
     * ├─ /table-4 (Hudi table, but should be excluded)
     * │   └─ /.hoodie
     *
     * This mock setup will be used to validate the table discovery process, ensuring that only valid tables
     * are discovered and that excluded paths are properly ignored.
     */

    when(asyncStorageClient.listAllFilesInDir(BASE_PATH))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(
                    generateFileObj("file1", false),
                    generateFileObj("table1/", true),
                    generateFileObj("excluded/", true),
                    generateFileObj("nested-folder/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "table1/"))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/"))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(
                    generateFileObj("table2/", true),
                    generateFileObj("excluded-table-3/", true),
                    generateFileObj("unrelated-folder1/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/table2/"))
        .thenReturn(
            CompletableFuture.completedFuture(
                Collections.singletonList(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/unrelated-folder1/"))
        .thenReturn(CompletableFuture.completedFuture(emptyList()));

    // paths to exclude
    String dirToExclude = BASE_PATH + "excluded/"; // excluding using an absolute path

    when(asyncStorageClient.listAllFilesInDir(BASE_PATH_2))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(
                    generateFileObj("tableWithId/", true), generateFileObj("table-4/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH_2 + "tableWithId/"))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH_2 + "table-4/"))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(generateFileObj(".hoodie", true))));
    String tableId = "11111";
    String basePath2ConfigWithTableId = BASE_PATH_2 + "#" + tableId;
    String basePath2ExplicitlyExcludedTable = BASE_PATH_2 + "table-4/";

    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getPathExclusionPatterns())
        .thenReturn(
            Optional.of(
                Arrays.asList(
                    dirToExclude, ".*excluded-table.*", basePath2ExplicitlyExcludedTable)));
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            Collections.singletonList(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        Collections.singletonList(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(
                                    Arrays.asList(
                                        BASE_PATH,
                                        basePath2ConfigWithTableId,
                                        basePath2ExplicitlyExcludedTable))
                                .build()))
                    .build()));

    // directly using StorageUtils as it is already tested and has basic helper functions
    tableDiscoveryService =
        new TableDiscoveryService(
            asyncStorageClient,
            new StorageUtils(),
            new ConfigProvider(config),
            ForkJoinPool.commonPool(),
            hudiMetadataExtractorMetrics);

    Set<Table> tableSet = tableDiscoveryService.discoverTables().get();
    List<Table> expectedResponseSet =
        Stream.of(
                Table.builder()
                    .absoluteTableUri(BASE_PATH + "table1/")
                    .databaseName(DATABASE)
                    .lakeName(LAKE)
                    .build(),
                Table.builder()
                    .absoluteTableUri(BASE_PATH + "nested-folder/table2/")
                    .databaseName(DATABASE)
                    .lakeName(LAKE)
                    .build(),
                Table.builder()
                    .absoluteTableUri(BASE_PATH_2 + "tableWithId/")
                    .tableId(tableId)
                    .databaseName(DATABASE)
                    .lakeName(LAKE)
                    .build())
            .sorted(Comparator.comparing(Table::getAbsoluteTableUri))
            .collect(Collectors.toList());
    assertIterableEquals(
        expectedResponseSet,
        tableSet.stream()
            .sorted(Comparator.comparing(Table::getAbsoluteTableUri))
            .collect(Collectors.toList()));

    // List will be called for:
    // s3://bucket/base_path/
    // s3://bucket/base_path/table1
    // s3://bucket/base_path/nested-folder
    // s3://bucket/base_path/nested-folder/table2
    // s3://bucket/base_path/nested-folder/unrelated-folder1
    // s3://bucket/base_path_2/
    // s3://bucket/base_path_2/tableWithId/
    // s3://bucket/base_path_2/table-4/
    verify(asyncStorageClient, times(8)).listAllFilesInDir(anyString());
  }

  @Test
  void testCaseWhereMoreThanOneDiscoveredTablesForTableId() {
    /*
     * Mock the asyncStorageClient to simulate the file system structure for testing the discovery of tables.
     * The simulated folder structure is as follows:
     *
     * s3://bucket/base_path
     * │
     * ├─ /table1 (Hudi table directory)
     * │   └─ /.hoodie (presence of this folder indicates a Hudi table)
     * │
     * ├─ /table2 (Hudi table directory)
     * │   └─ /.hoodie (presence of this folder indicates a Hudi table)
     *
     */
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH))
        .thenReturn(
            CompletableFuture.completedFuture(
                Arrays.asList(generateFileObj("table1/", true), generateFileObj("table2/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "table1/"))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "table2/"))
        .thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(generateFileObj(".hoodie", true))));

    String tableId = "11111";
    String basePathConfigWithTableId = BASE_PATH + "#" + tableId;

    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            Collections.singletonList(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        Collections.singletonList(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(Collections.singletonList(basePathConfigWithTableId))
                                .build()))
                    .build()));

    // directly using StorageUtils as it is already tested and has basic helper functions
    tableDiscoveryService =
        new TableDiscoveryService(
            asyncStorageClient,
            new StorageUtils(),
            new ConfigProvider(config),
            ForkJoinPool.commonPool(),
            hudiMetadataExtractorMetrics);

    Set<Table> discoveredTables = tableDiscoveryService.discoverTables().join();
    assertEquals(emptySet(), discoveredTables);
  }

  @Test
  void testWithInvalidBasePath() {
    String invalidBasePath = "/this-is-some-invalid-path";
    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getPathExclusionPatterns()).thenReturn(Optional.of(emptyList()));
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            Collections.singletonList(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        Collections.singletonList(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(Collections.singletonList(invalidBasePath))
                                .build()))
                    .build()));

    // calling the real method would throw the exception
    S3AsyncStorageClient s3AsyncStorageClient =
        new S3AsyncStorageClient(
            mock(S3AsyncClientProvider.class),
            new StorageUtils(),
            Executors.newSingleThreadExecutor());

    tableDiscoveryService =
        new TableDiscoveryService(
            s3AsyncStorageClient,
            new StorageUtils(),
            new ConfigProvider(config),
            ForkJoinPool.commonPool(),
            hudiMetadataExtractorMetrics);
    assertEquals(emptySet(), tableDiscoveryService.discoverTables().join());
  }

  @Test
  void testTableDiscoveryWithAsyncExceptions() {
    String basePath = "s3://some-bucket/some-path";
    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getPathExclusionPatterns()).thenReturn(Optional.of(emptyList()));
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            Collections.singletonList(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        Collections.singletonList(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(Collections.singletonList(basePath))
                                .build()))
                    .build()));
    CompletableFuture<List<File>> failedFuture = new CompletableFuture<>();
    when(asyncStorageClient.listAllFilesInDir(basePath)).thenReturn(failedFuture);
    failedFuture.completeExceptionally(new RuntimeException("some-error"));

    tableDiscoveryService =
        new TableDiscoveryService(
            asyncStorageClient,
            new StorageUtils(),
            new ConfigProvider(config),
            ForkJoinPool.commonPool(),
            hudiMetadataExtractorMetrics);
    assertEquals(emptySet(), tableDiscoveryService.discoverTables().join());
  }

  private File generateFileObj(String fileName, boolean isDirectory) {
    return File.builder()
        .filename(fileName)
        .isDirectory(isDirectory)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }
}
