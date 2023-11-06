package com.onehouse;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.onehouse.api.AsyncHttpClientWithRetry;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigLoader;
import com.onehouse.config.configv1.ConfigV1;
import com.onehouse.config.configv1.MetadataExtractorConfig;
import com.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static TableDiscoveryAndUploadJob job;
  private static AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    logger.info("Starting table metadata extractor service");
    if (args.length != 1) {
      System.err.println("Usage: java Main <config-file-path>");
      System.exit(1);
    }

    String configFilePath = args[0];
    Config config = ConfigLoader.loadConfig(configFilePath);

    Injector injector = Guice.createInjector(new RuntimeModule(config));
    job = injector.getInstance(TableDiscoveryAndUploadJob.class);
    asyncHttpClientWithRetry = injector.getInstance(AsyncHttpClientWithRetry.class);

    Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdownJob));

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
