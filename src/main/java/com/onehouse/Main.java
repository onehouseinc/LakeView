package com.onehouse;

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

  private static TableDiscoveryAndUploadJob job;
  private static AsyncHttpClientWithRetry asyncHttpClientWithRetry;

  public static void main(String[] args) {
    log.info("Starting table metadata extractor service");
    CliParser parser = new CliParser();
    ConfigLoader configLoader = new ConfigLoader();
    Config config = null;
    try {
      parser.parse(args);
      String configFilePath = parser.getConfigFilePath();
      String configYamlString = parser.getConfigYamlString();
      if (configFilePath != null) {
        config = configLoader.loadConfigFromConfigFile(configFilePath);
      } else if (configYamlString != null) {
        config = configLoader.loadConfigFromString(configYamlString);
      } else {
        log.error("No configuration provided. Please specify either a file path or a YAML string.");
        System.exit(1);
      }
    } catch (ParseException e) {
      log.error("Failed to parse command line arguments", e);
      System.exit(1);
    }

    Injector injector = Guice.createInjector(new RuntimeModule(config));
    job = injector.getInstance(TableDiscoveryAndUploadJob.class);
    asyncHttpClientWithRetry = injector.getInstance(AsyncHttpClientWithRetry.class);

    Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdownJob));

    // currently we only support one config version
    MetadataExtractorConfig.JobRunMode jobRunMode =
        ((ConfigV1) config).getMetadataExtractorConfig().getJobRunMode();
    if (MetadataExtractorConfig.JobRunMode.CONTINUOUS.equals(jobRunMode)) {
      job.runInContinuousMode();
    } else {
      job.runOnce();
    }
  }

  private static void shutdownJob() {
    if (job != null) {
      job.shutdown();
      asyncHttpClientWithRetry.shutdownScheduler();
    }
  }
}
