package com.onehouse.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
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
            Filesystem.GCS),
        Arguments.of(
            "src/test/resources/config_test_resources/validConfigV1S3Filesystem.yaml",
            Filesystem.S3));
  }

  @ParameterizedTest
  @MethodSource("provideValidConfigPaths")
  void testLoadingValidConfig(String configPath, Filesystem filesystem) {
    Config config = configLoader.loadConfig(configPath);
    assertTrue(config instanceof ConfigV1);
    assertEquals(config, getValidConfigV1Obj(filesystem));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "src/test/resources/config_test_resources/invalidConfigV1MissingFileSystemConfiguration.yaml",
        "src/test/resources/config_test_resources/invalidConfigV1MissingOnehouseClientConfig.yaml"
      })
  void testLoadingInValidConfig(String configPath) {
    assertThrows(RuntimeException.class, () -> configLoader.loadConfig(configPath));
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
        .region("us-west-2")
        .userUuid("KypBAFHYqAevFFeweB5UP2")
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
                .projectId("projectId")
                .gcpServiceAccountKeyPath("valid/path/service_account.json")
                .build())
        .build();
  }

  private MetadataExtractorConfig getValidMetadataExtractorConfig(Filesystem filesystemType) {
    String fileSystemPrefix = Filesystem.S3.equals(filesystemType) ? "s3://" : "gs://";
    String pathToExclude = fileSystemPrefix + "lake_bucket/tables/excluded";
    String basePath = fileSystemPrefix + "lake_bucket/tables";

    return MetadataExtractorConfig.builder()
        .pathsToExclude(Optional.of(List.of(pathToExclude)))
        .parserConfig(
            List.of(
                ParserConfig.builder()
                    .lake("lake1")
                    .databases(
                        List.of(
                            Database.builder()
                                .name("database1")
                                .basePaths(List.of(basePath))
                                .build()))
                    .build()))
        .build();
  }

  private ConfigV1 getValidConfigV1Obj(Filesystem filesystem) {
    return ConfigV1.builder()
        .version("V1")
        .onehouseClientConfig(getValidOnehouseClientConfig())
        .fileSystemConfiguration(getValidFileSystemConfig(filesystem))
        .metadataExtractorConfig(getValidMetadataExtractorConfig(filesystem))
        .build();
  }
}
