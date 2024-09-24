package com.onehouse.config;

import static org.mockito.Mockito.when;

import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import com.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConfigRefresherTest {
  private final ConfigLoader configLoader = new ConfigLoader();
  @Mock private AsyncStorageClient storageClient;

  @Test
  void testConfigOverride() throws Exception {

    String extractorConfigPath = "s3://bucket1/extractor.yaml";
    String extractorConfig =
        getFileAsString(
            "src/test/resources/config_test_resources/validExtractorConfigV1S3Filesystem.yaml");
    when(storageClient.readFileAsBytes(extractorConfigPath))
        .thenReturn(
            CompletableFuture.completedFuture(extractorConfig.getBytes(StandardCharsets.UTF_8)));
    String baseConfigPath =
        "src/test/resources/config_test_resources/validConfigWithNoLakeDatabase.yaml";
    Config baseConfig = configLoader.loadConfigFromConfigFile(baseConfigPath);
    ConfigProvider configProvider = new ConfigProvider(baseConfig);
    ConfigRefresher configRefresher =
        new ConfigRefresher(
            getFileAsString(baseConfigPath),
            extractorConfigPath,
            storageClient,
            configLoader,
            configProvider);
    configRefresher.start();
    Config config = configProvider.getConfig();
    MetadataExtractorConfig metadataExtractorConfig = config.getMetadataExtractorConfig();
    Assertions.assertEquals(1, metadataExtractorConfig.getParserConfig().size());
    ParserConfig parserConfig = metadataExtractorConfig.getParserConfig().get(0);
    Assertions.assertEquals("lakeOverride1", parserConfig.getLake());
    Assertions.assertEquals(1, parserConfig.getDatabases().size());
    Assertions.assertEquals("databaseOverride", parserConfig.getDatabases().get(0).getName());
  }

  private static String getFileAsString(String filePath) throws IOException {
    try (InputStream in = Files.newInputStream(Paths.get(filePath))) {
      return IOUtils.toString(in, StandardCharsets.UTF_8);
    }
  }
}
