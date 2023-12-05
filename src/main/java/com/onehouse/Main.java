package com.onehouse;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.cli_parser.CliParser;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigLoader;
import com.onehouse.config.models.configv1.ConfigV1;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;

@Slf4j
public class Main {

  private TableDiscoveryAndUploadJob job;
  private AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private final CliParser parser;
  private final ConfigLoader configLoader;

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
    log.info("Starting table metadata extractor service");
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

    Injector injector = Guice.createInjector(new RuntimeModule(config));
    job = injector.getInstance(TableDiscoveryAndUploadJob.class);
    asyncHttpClientWithRetry = injector.getInstance(AsyncHttpClientWithRetry.class);

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownJob));

    runJob(config);
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
        job.runInContinuousMode();
      } else {
        job.runOnce();
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    } finally {
      // explicitly shutdown scheduler so that JVM can exit
      asyncHttpClientWithRetry.shutdownScheduler();
    }
  }

  @VisibleForTesting
  void shutdownJob() {
    if (job != null) {
      job.shutdown();
    }
  }
}
