package com.onehouse.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onehouse.config.models.common.FileSystemConfiguration;
import com.onehouse.config.models.common.GCSConfig;
import com.onehouse.config.models.common.OnehouseClientConfig;
import com.onehouse.config.models.common.S3Config;
import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.config.models.configv1.Database;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ConfigLoaderTest {

  private ConfigLoader configLoader;

  @BeforeEach
  void setup() {
    configLoader = new ConfigLoader();
  }

  static Stream<Arguments> provideValidConfigPaths() {
    return Stream.of(
        Arguments.of(
            "src/test/resources/config_test_resources/validConfigV1GCSFilesystem.yaml",
            Filesystem.GCS,
            "lake1",
            "database1"),
        Arguments.of(
            "src/test/resources/config_test_resources/validConfigV1S3Filesystem.yaml",
            Filesystem.S3,
            "lake1",
            "database1"),
        Arguments.of(
            "src/test/resources/config_test_resources/validConfigWithNoLakeDatabase.yaml",
            Filesystem.S3,
            null,
            null));
  }

  @ParameterizedTest
  @MethodSource("provideValidConfigPaths")
  void testLoadingValidConfig(
      String configPath, Filesystem filesystem, String lake, String database) {
    Config config = configLoader.loadConfigFromConfigFile(configPath);
    assertTrue(config instanceof ConfigV1);
    assertEquals(getValidConfigV1Obj(filesystem, lake, database), config);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "src/test/resources/config_test_resources/invalidConfigV1MissingFileSystemConfiguration.yaml",
        "src/test/resources/config_test_resources/invalidConfigV1MissingOnehouseClientConfig.yaml"
      })
  void testLoadingInvalidConfig(String configPath) {
    assertThrows(RuntimeException.class, () -> configLoader.loadConfigFromConfigFile(configPath));
  }

  @Test
  void testLoadingYamlFromString() {
    // minifying using https://onlineyamltools.com/minify-yaml
    String yamlString =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    Config config = configLoader.loadConfigFromString(yamlString);
    assertEquals(getValidConfigV1Obj(Filesystem.S3, "lake1", "database1"), config);
  }

  public static Stream<Arguments> getCredentialsFileParameters() {
    return Stream.of(
        Arguments.of(true, ".yaml"), Arguments.of(false, ".yaml"), Arguments.of(true, ".json"));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("getCredentialsFileParameters")
  void testLoadingClientCredentialsFromFile(boolean createTempFile, String configFileExtension) {
    // minifying using https://onlineyamltools.com/minify-yaml
    String yamlString =
        "{version: V1, onehouseClientConfig: {file: \"%s\"}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    yamlString =
        String.format(
            yamlString,
            absolutePath
                + "/config_test_resources/onehouse-client-credentials"
                + configFileExtension);
    Config config;
    if (createTempFile) {
      Path tempFile = Files.createTempFile("temp", ".yaml");
      Files.write(tempFile, yamlString.getBytes());
      config = configLoader.loadConfigFromConfigFile(tempFile.toAbsolutePath().toString());
      Files.deleteIfExists(tempFile);
    } else {
      config = configLoader.loadConfigFromString(yamlString);
    }
    ConfigV1 expectedConfig = getValidConfigV1Obj(Filesystem.S3, "lake1", "database1");
    assertEquals(
        expectedConfig.getOnehouseClientConfig().getProjectId(),
        config.getOnehouseClientConfig().getProjectId());
    assertEquals(
        expectedConfig.getOnehouseClientConfig().getApiKey(),
        config.getOnehouseClientConfig().getApiKey());
    assertEquals(
        expectedConfig.getOnehouseClientConfig().getApiSecret(),
        config.getOnehouseClientConfig().getApiSecret());
    assertEquals(
        expectedConfig.getOnehouseClientConfig().getUserId(),
        config.getOnehouseClientConfig().getUserId());
    assertEquals("some-request-id", config.getOnehouseClientConfig().getRequestId());
    assertEquals("us-west-2", config.getOnehouseClientConfig().getRegion());
  }

  @Test
  void testMissingFieldsInOnehouseClientConfig() {
    // minifying using https://onlineyamltools.com/minify-yaml
    String yamlString =
        "{version: V1, onehouseClientConfig: {}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> configLoader.loadConfigFromString(yamlString));
    assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    assertEquals(
        "Missing config params: projectId, apiKey, apiSecret, userId",
        exception.getCause().getMessage());
  }

  @Test
  void testLoadingInvalidYamlFromString() {
    // missing onehouseClientConfig
    String yamlString =
        "{version: V1, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    assertThrows(RuntimeException.class, () -> configLoader.loadConfigFromString(yamlString));

    // missing onehouseClientConfig
    String yamlStringWithInvalidActiveTimelineBatchSize =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {presignedUrlRequestBatchSizeActiveTimeline: -1, pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> configLoader.loadConfigFromString(yamlStringWithInvalidActiveTimelineBatchSize));
    assertEquals(
        "presignedUrlRequestBatchSizeActiveTimeline should be a positive integer",
        exception.getCause().getMessage());

    String yamlStringWithInvalidArchiveTimelineBatchSize =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {presignedUrlRequestBatchSizeArchivedTimeline: -1, pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    exception =
        assertThrows(
            RuntimeException.class,
            () -> configLoader.loadConfigFromString(yamlStringWithInvalidArchiveTimelineBatchSize));
    assertEquals(
        "presignedUrlRequestBatchSizeArchivedTimeline should be a positive integer",
        exception.getCause().getMessage());

    String yamlStringWithInvalidMetadataSyncDurationSeconds =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {processTableMetadataSyncDurationSeconds: -1, pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    exception =
        assertThrows(
            RuntimeException.class,
            () ->
                configLoader.loadConfigFromString(
                    yamlStringWithInvalidMetadataSyncDurationSeconds));
    assertEquals(
        "processTableMetadataSyncDurationSeconds should be a positive integer",
        exception.getCause().getMessage());

    String yamlStringWithInvalidTableDiscoveryIntervalMinutes =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {tableDiscoveryIntervalMinutes: -1, pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    exception =
        assertThrows(
            RuntimeException.class,
            () ->
                configLoader.loadConfigFromString(
                    yamlStringWithInvalidTableDiscoveryIntervalMinutes));
    assertEquals(
        "tableDiscoveryIntervalMinutes should be a positive integer",
        exception.getCause().getMessage());

    String yamlStringWithInvalidTableMetadataUploadIntervalMinutes =
        "{version: V1, onehouseClientConfig: {projectId: 0c043996-9e42-4904-95b9-f98918ebeda4, apiKey: WJ3wiaZLsX0mDrrcw234akQ==, apiSecret: /v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=, userId: KypBAFHYqAevFFeweB5UP2}, fileSystemConfiguration: {s3Config: {region: us-west-2}}, metadataExtractorConfig: {tableMetadataUploadIntervalMinutes: -1, pathExclusionPatterns: ['s3://lake_bucket/tables/excluded'], parserConfig: [{lake: lake1, databases: [{name: database1, basePaths: ['s3://lake_bucket/tables']}]}]}}";
    exception =
        assertThrows(
            RuntimeException.class,
            () ->
                configLoader.loadConfigFromString(
                    yamlStringWithInvalidTableMetadataUploadIntervalMinutes));
    assertEquals(
        "tableMetadataUploadIntervalMinutes should be a positive integer",
        exception.getCause().getMessage());
  }

  enum Filesystem {
    S3,
    GCS
  }

  private OnehouseClientConfig getValidOnehouseClientConfig() {
    return OnehouseClientConfig.builder()
        .projectId("0c043996-9e42-4904-95b9-f98918ebeda4")
        .apiKey("WJ3wiaZLsX0mDrrcw234akQ==")
        .apiSecret("/v+WFnHYscwgwerPn91VK+6Lrp2/11Bp0ojKp+fhOAOA=")
        .userId("KypBAFHYqAevFFeweB5UP2")
        .build();
  }

  private FileSystemConfiguration getValidFileSystemConfig(Filesystem filesystemType) {
    if (Filesystem.S3.equals(filesystemType)) {
      return FileSystemConfiguration.builder()
          .s3Config(S3Config.builder().region("us-west-2").build())
          .build();
    }
    return FileSystemConfiguration.builder()
        .gcsConfig(
            GCSConfig.builder()
                .projectId(Optional.of("projectId"))
                .gcpServiceAccountKeyPath(Optional.of("valid/path/service_account.json"))
                .build())
        .build();
  }

  private MetadataExtractorConfig getValidMetadataExtractorConfig(
      Filesystem filesystemType, String lake, String database) {
    String fileSystemPrefix = Filesystem.S3.equals(filesystemType) ? "s3://" : "gs://";
    String pathToExclude = fileSystemPrefix + "lake_bucket/tables/excluded";
    String basePath = fileSystemPrefix + "lake_bucket/tables";

    return MetadataExtractorConfig.builder()
        .pathExclusionPatterns(Optional.of(Collections.singletonList(pathToExclude)))
        .parserConfig(
            Collections.singletonList(
                ParserConfig.builder()
                    .lake(lake)
                    .databases(
                        Collections.singletonList(
                            Database.builder()
                                .name(database)
                                .basePaths(Collections.singletonList(basePath))
                                .build()))
                    .build()))
        .build();
  }

  private ConfigV1 getValidConfigV1Obj(Filesystem filesystem, String lake, String database) {
    return ConfigV1.builder()
        .version("V1")
        .onehouseClientConfig(getValidOnehouseClientConfig())
        .fileSystemConfiguration(getValidFileSystemConfig(filesystem))
        .metadataExtractorConfig(getValidMetadataExtractorConfig(filesystem, lake, database))
        .build();
  }
}
