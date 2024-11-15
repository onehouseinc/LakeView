package ai.onehouse.lakeview.sync;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LakeviewSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() throws IOException {
    FileSystem fileSystem = FileSystem.get(URI.create("file://" + BASE_PATH), new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  private static Stream<Arguments> getArguments() {
    return Stream.of(Arguments.of("s3", false),
        Arguments.of("s3", true),
        Arguments.of("gs", false),
        Arguments.of("gs", true));
  }

  @ParameterizedTest
  @MethodSource("getArguments")
  void testSyncTool(String fileSystem, boolean failSyncing) throws IOException {
    List<ParserConfig> expectedParserConfigs = new ArrayList<>();
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-1")
        .databases(Collections.singletonList(Database.builder()
            .name("database-1")
            .basePaths(Collections.singletonList(fileSystem + "://user-bucket/lake-1/database-1/table-2"))
            .build()))
        .build());

    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream(String.format("/lakeview-sync-%s.properties", fileSystem)));
    try (MockedConstruction<TableDiscoveryAndUploadJob> mockedConstruction =
             mockConstruction(TableDiscoveryAndUploadJob.class, (tableDiscoveryAndUploadJob, context) -> {
               if (failSyncing) {
                 doThrow(new RuntimeException()).when(tableDiscoveryAndUploadJob).runOnce();
               } else {
                 doNothing().when(tableDiscoveryAndUploadJob).runOnce();
               }
             });
         LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(properties, hadoopConf)) {
      Config config = lakeviewSyncTool.getConfig();
      assertNotNull(config);
      assertEquals(new HashSet<>(expectedParserConfigs),
          new HashSet<>(config.getMetadataExtractorConfig().getParserConfig()));
      java.util.Optional<List<String>> pathExclusionPatterns = config.getMetadataExtractorConfig().getPathExclusionPatterns();
      assertTrue(pathExclusionPatterns.isPresent());
      assertEquals(2, pathExclusionPatterns.get().size());
      if (fileSystem.equals("s3")) {
        assertNotNull(config.getFileSystemConfiguration().getS3Config());
      } else {
        assertNotNull(config.getFileSystemConfiguration().getGcsConfig());
      }

      assertDoesNotThrow(lakeviewSyncTool::syncHoodieTable);

      List<TableDiscoveryAndUploadJob> constructedObjects = mockedConstruction.constructed();
      assertEquals(1, constructedObjects.size());
      TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob = constructedObjects.get(0);
      verify(tableDiscoveryAndUploadJob, times(1)).runOnce();
    }
  }

  @Test
  void testSyncToolTimeout() throws IOException {
    List<ParserConfig> expectedParserConfigs = new ArrayList<>();
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-1")
        .databases(Collections.singletonList(Database.builder()
            .name("database-1")
            .basePaths(Collections.singletonList("s3://user-bucket/lake-1/database-1/table-2"))
            .build()))
        .build());

    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/lakeview-sync-s3.properties"));
    int timeoutInSeconds = 1;
    Path tempFile = Files.createTempFile("temp", ".txt");
    try (MockedConstruction<TableDiscoveryAndUploadJob> mockedConstruction =
             mockConstruction(TableDiscoveryAndUploadJob.class, (tableDiscoveryAndUploadJob, context) -> {
               doAnswer(invocationOnMock -> {
                 // wait for timeout to exceed & then delete the temp file
                 await().atMost(Duration.ofSeconds(timeoutInSeconds + 10)).until(() -> !Files.exists(tempFile));
                 Files.deleteIfExists(tempFile);
                 return null;
               }).when(tableDiscoveryAndUploadJob).runOnce();
             });
         LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(properties, hadoopConf)) {
      Config config = lakeviewSyncTool.getConfig();
      assertNotNull(config);
      assertEquals(new HashSet<>(expectedParserConfigs),
          new HashSet<>(config.getMetadataExtractorConfig().getParserConfig()));
      java.util.Optional<List<String>> pathExclusionPatterns = config.getMetadataExtractorConfig().getPathExclusionPatterns();
      assertTrue(pathExclusionPatterns.isPresent());
      assertEquals(2, pathExclusionPatterns.get().size());
      assertNotNull(config.getFileSystemConfiguration().getS3Config());

      assertDoesNotThrow(lakeviewSyncTool::syncHoodieTable);

      List<TableDiscoveryAndUploadJob> constructedObjects = mockedConstruction.constructed();
      assertEquals(1, constructedObjects.size());
      TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob = constructedObjects.get(0);
      verify(tableDiscoveryAndUploadJob, times(1)).runOnce();

      // verify that the temp file is still present as the future got cancelled due to timeout
      assertTrue(Files.exists(tempFile));
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  void testSyncToolDisabled() {
    Properties properties = new Properties();
    try (LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(properties, hadoopConf)) {
      assertNull(lakeviewSyncTool.getConfig());
      // no explicit mocks needed as the syncing is disabled by default
      lakeviewSyncTool.syncHoodieTable();
    }
  }

  private static Stream<Arguments> getIncorrectProperties() {
    return Stream.of(Arguments.of("lakeview-sync-no-fs.properties",
            "Couldn't find any properties related to file system"),
        Arguments.of("lakeview-sync-no-lake.properties",
            "Couldn't find any lake/database associated with the current table in the configuration"));
  }

  @ParameterizedTest
  @MethodSource("getIncorrectProperties")
  void testSyncToolInvalidConfig(String propertiesFile, String errorMessage) throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/" + propertiesFile));
    try (LakeviewSyncTool ignored = new LakeviewSyncTool(properties, hadoopConf)) {
      fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      assertEquals(errorMessage, e.getMessage());
    }
  }

  @Test
  void testRunner() throws IOException {
    LakeviewSyncTool.main(new String[]{"--help"});

    int timeoutInSeconds = 1;
    Path tempFile = Files.createTempFile("temp", ".txt");
    try (MockedConstruction<TableDiscoveryAndUploadJob> mockedConstruction =
             mockConstruction(TableDiscoveryAndUploadJob.class, (tableDiscoveryAndUploadJob, context) ->
                 doAnswer(invocationOnMock -> {
                   // wait for timeout to exceed & then delete the temp file
                   await().atMost(Duration.ofSeconds(timeoutInSeconds + 10)).until(() -> !Files.exists(tempFile));
                   Files.deleteIfExists(tempFile);
                   return null;
                 }).when(tableDiscoveryAndUploadJob).runOnce())) {

      assertDoesNotThrow(() -> LakeviewSyncTool.main(new String[]{
          "--project-id", "xyz",
          "--api-key", "my-api-key",
          "--api-secret", "my-api-secret",
          "--userid", "my-userid",
          "--lake-paths", "lake1.databases.database1.basePaths=s3://user-bucket/lake-1/database-1/table-1,"
          + "s3://user-bucket/lake-1/database-1/table-2",
          "--lake-paths", "lake1.databases.database2.basePaths=s3://user-bucket/lake-1/database-2/table-1,"
          + "s3://user-bucket/lake-1/database-2/table-2",
          "--base-path", "s3://user-bucket/lake-1/database-1/table-2",
          "--s3-region", "us-west-2",
          "--timeout", String.valueOf(timeoutInSeconds)
      }));

      List<TableDiscoveryAndUploadJob> constructedObjects = mockedConstruction.constructed();
      assertEquals(1, constructedObjects.size());
      TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob = constructedObjects.get(0);
      verify(tableDiscoveryAndUploadJob, times(1)).runOnce();

      // verify that the temp file is still present as the future got cancelled due to timeout
      assertTrue(Files.exists(tempFile));
      Files.deleteIfExists(tempFile);
    }
  }
}
