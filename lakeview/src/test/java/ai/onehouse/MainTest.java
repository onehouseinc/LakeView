package ai.onehouse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import ai.onehouse.metrics.LakeViewExtractorMetrics;
import com.google.inject.Guice;
import com.google.inject.Injector;
import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.cli_parser.CliParser;
import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigLoader;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.configv1.ConfigV1;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import ai.onehouse.metrics.MetricsModule;
import ai.onehouse.metrics.MetricsServer;
import ai.onehouse.storage.AsyncStorageClient;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MainTest {

  @Mock private CliParser mockParser;
  @Mock private ConfigLoader mockConfigLoader;
  @Mock private ConfigProvider mockConfigProvider;
  @Mock private AsyncStorageClient mockAsyncStorageClient;
  @Mock private Injector mockInjector;
  @Mock private TableDiscoveryAndUploadJob mockJob;
  @Mock private AsyncHttpClientWithRetry mockAsyncHttpClientWithRetry;
  @Mock private ConfigV1 mockConfig;
  @Mock private MetricsServer mockMetricsServer;
  @Mock private LakeViewExtractorMetrics lakeViewExtractorMetrics;
  MockedStatic<Guice> guiceMockedStatic;

  private Main main;

  @BeforeEach
  void setUp() {
    guiceMockedStatic = mockStatic(Guice.class);
    main = new Main(mockParser, mockConfigLoader);
  }

  @AfterEach
  void shutdown() {
    guiceMockedStatic.close();
  }

  @Test
  void testLoadConfigFromFileAndRunOnce() {
    String[] args = {"-p", "configFilePath"};
    when(mockParser.getConfigFilePath()).thenReturn("configFilePath");
    when(mockConfigLoader.loadConfigFromConfigFile(anyString())).thenReturn(mockConfig);
    when(mockConfigProvider.getConfig()).thenReturn(mockConfig);
    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
                .parserConfig(Collections.emptyList())
                .waitTimeBeforeShutdown(0)
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    when(mockInjector.getInstance(MetricsServer.class)).thenReturn(mockMetricsServer);
    when(mockInjector.getInstance(ConfigProvider.class)).thenReturn(mockConfigProvider);
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class), any(MetricsModule.class)))
        .thenReturn(mockInjector);
    main.start(args);

    verify(mockConfigLoader).loadConfigFromConfigFile("configFilePath");
    verify(mockJob).runOnce();
    verifyShutdown();
  }

  @Test
  void testLoadConfigFromFileAndRunOnceFail() {
    String[] args = {"-p", "configFilePath"};
    when(mockParser.getConfigFilePath()).thenReturn("configFilePath");
    when(mockConfigLoader.loadConfigFromConfigFile(anyString())).thenReturn(mockConfig);
    when(mockConfigProvider.getConfig()).thenReturn(mockConfig);
    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
                .parserConfig(Collections.emptyList())
                .waitTimeBeforeShutdown(0)
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    when(mockInjector.getInstance(MetricsServer.class)).thenReturn(mockMetricsServer);
    when(mockInjector.getInstance(ConfigProvider.class)).thenReturn(mockConfigProvider);
    doThrow(new RuntimeException()).when(mockJob).runOnce();
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class), any(MetricsModule.class)))
        .thenReturn(mockInjector);
    main.start(args);

    verify(mockConfigLoader).loadConfigFromConfigFile("configFilePath");
    verify(mockJob).runOnce();
    verifyShutdown();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConfigOverride(boolean isFailure) throws IOException {
    String[] args = {"-p", "configFilePath"};
    String baseConfigPath =
        "src/test/resources/config_test_resources/BaseConfigWithExternalExtractorConfigPath.yaml";
    String extractorConfigPath =
        "src/test/resources/config_test_resources/validExtractorConfigV1S3Filesystem.yaml";
    when(mockParser.getConfigFilePath()).thenReturn(baseConfigPath);
    String extractorConfig = getFileAsString(extractorConfigPath);

    if (isFailure) {
      when(mockAsyncStorageClient.readFileAsBytes(extractorConfigPath)).thenThrow(new RuntimeException("Could not read file"));
    } else {
      when(mockAsyncStorageClient.readFileAsBytes(extractorConfigPath))
          .thenReturn(
              CompletableFuture.completedFuture(extractorConfig.getBytes(StandardCharsets.UTF_8)));
    }

    ConfigLoader configLoader = new ConfigLoader();
    Config baseConfig = configLoader.loadConfigFromConfigFile(baseConfigPath);
    ConfigProvider configProvider = new ConfigProvider(baseConfig);

    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    when(mockInjector.getInstance(ConfigProvider.class)).thenReturn(configProvider);
    when(mockInjector.getInstance(AsyncStorageClient.class)).thenReturn(mockAsyncStorageClient);
    when(mockInjector.getInstance(LakeViewExtractorMetrics.class)).thenReturn(lakeViewExtractorMetrics);
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class), any(MetricsModule.class)))
        .thenReturn(mockInjector);
    when(mockInjector.getInstance(MetricsServer.class)).thenReturn(mockMetricsServer);
    Main main = new Main(mockParser, configLoader);
    main.start(args);

    verify(mockInjector, times(1)).getInstance(AsyncStorageClient.class);
    verify(mockAsyncStorageClient, times(1)).readFileAsBytes(extractorConfigPath);

    verify(mockJob).runOnce();
    if (isFailure) {
      verify(lakeViewExtractorMetrics).incrementFailedOverrideConfigCounter();
    }
    verifyShutdown();
  }

  @Test
  void testLoadConfigFromStringAndRunContinuous() {
    String[] args = {"-c", "configYamlString"};

    when(mockParser.getConfigYamlString()).thenReturn("configYamlString");
    when(mockConfigLoader.loadConfigFromString(anyString())).thenReturn(mockConfig);
    when(mockConfigProvider.getConfig()).thenReturn(mockConfig);

    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.CONTINUOUS)
                .waitTimeBeforeShutdown(0)
                .parserConfig(Collections.emptyList())
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    when(mockInjector.getInstance(MetricsServer.class)).thenReturn(mockMetricsServer);
    when(mockInjector.getInstance(ConfigProvider.class)).thenReturn(mockConfigProvider);
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class), any(MetricsModule.class)))
        .thenReturn(mockInjector);
    main.start(args);
    main.shutdown(mockConfig);

    verify(mockConfigLoader).loadConfigFromString("configYamlString");
    verify(mockJob).runInContinuousMode(mockConfig);
    verifyShutdown();
  }

  @Test
  void testHelpOption() {
    String[] args = {"-h"};
    when(mockParser.isHelpRequested()).thenReturn(true);
    main.start(args);
  }

  private void verifyShutdown() {
    verify(mockJob).shutdown();
    verify(mockAsyncHttpClientWithRetry).shutdownScheduler();
    verify(mockMetricsServer).shutdown();
  }

  private static String getFileAsString(String filePath) throws IOException {
    try (InputStream in = Files.newInputStream(Paths.get(filePath))) {
      return IOUtils.toString(in, StandardCharsets.UTF_8);
    }
  }
}
