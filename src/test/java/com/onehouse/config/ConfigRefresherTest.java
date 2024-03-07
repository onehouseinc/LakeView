package com.onehouse.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;

import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.config.models.configv1.ParserConfig;
import com.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@ExtendWith(MockitoExtension.class)
public class ConfigRefresherTest {
  private final ConfigLoader configLoader = new ConfigLoader();
  private final String baseConfigPath =
      "src/test/resources/config_test_resources/validConfigWithNoLakeDatabase.yaml";
  private final String extractorConfigPath = "s3://bucket1/extractor.yaml";
  @Mock private AsyncStorageClient storageClient;

  public static Stream<Arguments> getExceptionsForNoConfigFile() {
    return Stream.of(
        Arguments.of(new IllegalAccessException()),
        Arguments.of(NoSuchKeyException.builder().build()),
        Arguments.of(new NoSuchElementException("Blob not found")));
  }

  @Test
  void testConfigOverride() throws Exception {
    String extractorConfig =
        getFileAsString(
            "src/test/resources/config_test_resources/validExtractorConfigV1S3Filesystem.yaml");
    when(storageClient.readFileAsBytes(extractorConfigPath))
        .thenReturn(
            CompletableFuture.completedFuture(extractorConfig.getBytes(StandardCharsets.UTF_8)));
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

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("getExceptionsForNoConfigFile")
  void testConfigFileNotFoundException(Exception exception) {
    Config baseConfig = configLoader.loadConfigFromConfigFile(baseConfigPath);
    ConfigProvider configProvider = new ConfigProvider(baseConfig);

    CompletableFuture<byte[]> future = new CompletableFuture<>();
    future.completeExceptionally(exception);
    when(storageClient.readFileAsBytes(extractorConfigPath)).thenReturn(future);

    ConfigRefresher configRefresher =
        new ConfigRefresher(
            getFileAsString(baseConfigPath),
            extractorConfigPath,
            storageClient,
            configLoader,
            configProvider);
    try {
      configRefresher.start();
      Config config = configProvider.getConfig();
      MetadataExtractorConfig metadataExtractorConfig = config.getMetadataExtractorConfig();
      Assertions.assertEquals(1, metadataExtractorConfig.getParserConfig().size());
      ParserConfig parserConfig = metadataExtractorConfig.getParserConfig().get(0);
      Assertions.assertNull(parserConfig.getLake());
      Assertions.assertEquals(1, parserConfig.getDatabases().size());
      Assertions.assertEquals(
          "s3://lake_bucket/tables", parserConfig.getDatabases().get(0).getBasePaths().get(0));
    } catch (Exception e) {
      assertInstanceOf(IllegalAccessException.class, e.getCause());
      assertEquals(exception, e.getCause());
    }
  }

  private static String getFileAsString(String filePath) throws IOException {
    try (InputStream in = Files.newInputStream(Paths.get(filePath))) {
      return IOUtils.toString(in, StandardCharsets.UTF_8);
    }
  }
}
