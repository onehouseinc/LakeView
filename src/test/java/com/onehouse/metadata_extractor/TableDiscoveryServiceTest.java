package com.onehouse.metadata_extractor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.config.models.configv1.Database;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.storage.AsyncStorageClient;
import com.onehouse.storage.StorageUtils;
import com.onehouse.storage.models.File;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  private static final String BASE_PATH = "s3://bucket/base_path/";
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
     * This mock setup will be used to validate the table discovery process, ensuring that only valid tables
     * are discovered and that excluded paths are properly ignored.
     */

    when(asyncStorageClient.listAllFilesInDir(BASE_PATH))
        .thenReturn(
            CompletableFuture.completedFuture(
                List.of(
                    generateFileObj("file1", false),
                    generateFileObj("table1/", true),
                    generateFileObj("excluded/", true),
                    generateFileObj("nested-folder/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "table1/"))
        .thenReturn(CompletableFuture.completedFuture(List.of(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/"))
        .thenReturn(
            CompletableFuture.completedFuture(
                List.of(
                    generateFileObj("table2/", true),
                    generateFileObj("excluded-table-3/", true),
                    generateFileObj("unrelated-folder1/", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/table2/"))
        .thenReturn(CompletableFuture.completedFuture(List.of(generateFileObj(".hoodie", true))));
    when(asyncStorageClient.listAllFilesInDir(BASE_PATH + "nested-folder/unrelated-folder1/"))
        .thenReturn(CompletableFuture.completedFuture(List.of()));

    // paths to exclude
    String dirToExclude = BASE_PATH + "excluded";
    String tableToExclude = BASE_PATH + "nested-folder/excluded-table-3";

    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getPathsToExclude())
        .thenReturn(Optional.of(List.of(dirToExclude, tableToExclude)));
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            List.of(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        List.of(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(List.of(BASE_PATH))
                                .build()))
                    .build()));

    // directly using StorageUtils as it is already tested and has basic helper functions
    tableDiscoveryService =
        new TableDiscoveryService(
            asyncStorageClient, new StorageUtils(), config, ForkJoinPool.commonPool());

    Set<Table> tableSet = tableDiscoveryService.discoverTables().get();
    List<Table> expectedResponseSet =
        Stream.of(
                Table.builder()
                    .absoluteTableUri(BASE_PATH + "table1/")
                    .relativeTablePath("table1")
                    .databaseName(DATABASE)
                    .lakeName(LAKE)
                    .build(),
                Table.builder()
                    .absoluteTableUri(BASE_PATH + "nested-folder/table2/")
                    .relativeTablePath("nested-folder/table2")
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
    verify(asyncStorageClient, times(5)).listAllFilesInDir(anyString());
  }

  @Test
  void testCaseWhereBasePathEqualsExcludedPath() {
    // paths to exclude
    String dirToExclude = BASE_PATH + "excluded";

    // parser config
    when(config.getMetadataExtractorConfig()).thenReturn(metadataExtractorConfig);
    when(metadataExtractorConfig.getPathsToExclude())
        .thenReturn(Optional.of(List.of(dirToExclude)));
    when(metadataExtractorConfig.getParserConfig())
        .thenReturn(
            List.of(
                ParserConfig.builder()
                    .lake(LAKE)
                    .databases(
                        List.of(
                            Database.builder()
                                .name(DATABASE)
                                .basePaths(List.of(dirToExclude))
                                .build()))
                    .build()));

    tableDiscoveryService =
        new TableDiscoveryService(
            asyncStorageClient, new StorageUtils(), config, ForkJoinPool.commonPool());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> tableDiscoveryService.discoverTables().join());
    assertEquals("Provided base path cannot be part of paths to excluded", exception.getMessage());
  }

  private File generateFileObj(String fileName, boolean isDirectory) {
    return File.builder()
        .filename(fileName)
        .isDirectory(isDirectory)
        .lastModifiedAt(Instant.EPOCH)
        .build();
  }
}
