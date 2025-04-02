package ai.onehouse;

import ai.onehouse.metrics.LakeViewExtractorMetrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.cli_parser.CliParser;
import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigLoader;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.ConfigRefresher;
import ai.onehouse.config.models.configv1.ConfigV1;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import ai.onehouse.metrics.MetricsModule;
import ai.onehouse.metrics.MetricsServer;
import ai.onehouse.storage.AsyncStorageClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class Main {

  private TableDiscoveryAndUploadJob job;
  private AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private MetricsServer metricsServer;
  private final CliParser parser;
  private final ConfigLoader configLoader;
  private ConfigRefresher configRefresher;
  private LakeViewExtractorMetrics lakeViewExtractorMetrics;

  public Main(CliParser parser, ConfigLoader configLoader) {
    this.parser = parser;
    this.configLoader = configLoader;
  }

  public static void main(String[] args) {
    CliParser parser = new CliParser();
    ConfigLoader configLoader = new ConfigLoader();

    Main main = new Main(parser, configLoader);
    main.start(args);
  }

  public void start(String[] args) {
    log.info("Starting LakeView extractor service");
    Config config = null;
    try {
      parser.parse(args);

      if (parser.isHelpRequested()) {
        return;
      }

      String configFilePath = parser.getConfigFilePath();
      String configYamlString = parser.getConfigYamlString();
      config = loadConfig(configFilePath, configYamlString);
    } catch (ParseException e) {
      log.error("Failed to parse command line arguments", e);
      System.exit(1);
    }

    Injector injector = Guice.createInjector(new RuntimeModule(config), new MetricsModule());
    job = injector.getInstance(TableDiscoveryAndUploadJob.class);
    asyncHttpClientWithRetry = injector.getInstance(AsyncHttpClientWithRetry.class);
    ConfigProvider configProvider = injector.getInstance(ConfigProvider.class);
    metricsServer = injector.getInstance(MetricsServer.class);
    lakeViewExtractorMetrics = injector.getInstance(LakeViewExtractorMetrics.class);

    // If metadata extractor config is provided externally, then override and refresh config
    // periodically.
    if (StringUtils.isNotBlank(config.getMetadataExtractorConfigPath())) {
      AsyncStorageClient storageClient = injector.getInstance(AsyncStorageClient.class);
      try {
        String baseConfigYaml = configLoader.convertConfigToString(config);
        configRefresher =
            new ConfigRefresher(
                baseConfigYaml,
                config.getMetadataExtractorConfigPath(),
                storageClient,
                configLoader,
                configProvider);
        configRefresher.start();
      } catch (Exception ex) {
        log.error("Failed to override metadata extractor config", ex);
        lakeViewExtractorMetrics.incrementFailedOverrideConfigCounter();
      }
    }

    runJob(configProvider.getConfig());
  }

  private Config loadConfig(String configFilePath, String configYamlString) {
    if (configFilePath != null) {
      return configLoader.loadConfigFromConfigFile(configFilePath);
    } else if (configYamlString != null) {
      return configLoader.loadConfigFromString(configYamlString);
    } else {
      log.error("No configuration provided. Please specify either a file path or a YAML string.");
      System.exit(1);
    }
    return null;
  }

  private void runJob(Config config) {
    try {
      MetadataExtractorConfig.JobRunMode jobRunMode =
          ((ConfigV1) config).getMetadataExtractorConfig().getJobRunMode();
      if (MetadataExtractorConfig.JobRunMode.CONTINUOUS.equals(jobRunMode)) {
        job.runInContinuousMode(config);
      } else {
        job.runOnce(config, 1);
        shutdown(config);
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      shutdown(config);
    }
  }

  @VisibleForTesting
  void shutdown(Config config) {
    if (config.getMetadataExtractorConfig().getJobRunMode().equals(MetadataExtractorConfig.JobRunMode.ONCE)) {
      log.info(String.format("Scheduling JVM shutdown after %d seconds",
          config.getMetadataExtractorConfig().getWaitTimeBeforeShutdown()));
      try {
        Thread.sleep(config.getMetadataExtractorConfig().getWaitTimeBeforeShutdown() * 1000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    asyncHttpClientWithRetry.shutdownScheduler();
    job.shutdown();
    metricsServer.shutdown();
    if (configRefresher != null) {
      configRefresher.shutdown();
    }
  }
}
