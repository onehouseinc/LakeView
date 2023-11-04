package com.onehouse.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onehouse.config.configV1.ConfigV1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConfigLoaderTest {

  private ConfigLoader configLoader;

  @BeforeEach
  void setup() {
    configLoader = new ConfigLoader();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/configs/validConfigV1GCSFilesystem.yaml",
        "/configs/validConfigV1S3Filesystem.yaml"
      })
  void testLoadingValidConfig(String configPath) {
    Config config = configLoader.loadConfig(configPath);
    assertTrue(config instanceof ConfigV1);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/configs/invalidConfigV1MissingFileSystemConfiguration.yaml",
        "/configs/invalidConfigV1MissingOnehouseClientConfig.yaml"
      })
  void testLoadingInValidConfig(String configPath) {
    assertThrows(RuntimeException.class, () -> configLoader.loadConfig(configPath));
  }
}
