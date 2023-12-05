package com.onehouse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.cli_parser.CliParser;
import com.onehouse.config.ConfigLoader;
import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MainTest {

  @Mock private CliParser mockParser;
  @Mock private ConfigLoader mockConfigLoader;
  @Mock private Injector mockInjector;
  @Mock private TableDiscoveryAndUploadJob mockJob;
  @Mock private AsyncHttpClientWithRetry mockAsyncHttpClientWithRetry;
  @Mock private ConfigV1 mockConfig;
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
    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
                .parserConfig(List.of())
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class)))
        .thenReturn(mockInjector);
    main.start(args);
    main.shutdownJob();

    verify(mockConfigLoader).loadConfigFromConfigFile("configFilePath");
    verify(mockJob).runOnce();
    verifyShutdown();
  }

  @Test
  void testLoadConfigFromFileAndRunOnceFail() {
    String[] args = {"-p", "configFilePath"};
    when(mockParser.getConfigFilePath()).thenReturn("configFilePath");
    when(mockConfigLoader.loadConfigFromConfigFile(anyString())).thenReturn(mockConfig);
    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
                .parserConfig(List.of())
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    doThrow(new RuntimeException()).when(mockJob).runOnce();
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class)))
        .thenReturn(mockInjector);
    main.start(args);
    main.shutdownJob();

    verify(mockConfigLoader).loadConfigFromConfigFile("configFilePath");
    verify(mockJob).runOnce();
    verifyShutdown();
  }

  @Test
  void testLoadConfigFromStringAndRunContinuous() {
    String[] args = {"-c", "configYamlString"};

    when(mockParser.getConfigYamlString()).thenReturn("configYamlString");
    when(mockConfigLoader.loadConfigFromString(anyString())).thenReturn(mockConfig);
    when(mockConfig.getMetadataExtractorConfig())
        .thenReturn(
            MetadataExtractorConfig.builder()
                .jobRunMode(MetadataExtractorConfig.JobRunMode.CONTINUOUS)
                .parserConfig(List.of())
                .build());
    when(mockInjector.getInstance(TableDiscoveryAndUploadJob.class)).thenReturn(mockJob);
    when(mockInjector.getInstance(AsyncHttpClientWithRetry.class))
        .thenReturn(mockAsyncHttpClientWithRetry);
    guiceMockedStatic
        .when(() -> Guice.createInjector(any(RuntimeModule.class)))
        .thenReturn(mockInjector);
    main.start(args);
    main.shutdownJob();

    verify(mockConfigLoader).loadConfigFromString("configYamlString");
    verify(mockJob).runInContinuousMode();
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
  }
}
