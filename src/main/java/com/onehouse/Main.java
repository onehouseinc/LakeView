package com.onehouse;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigLoader;
import com.onehouse.metadataExtractor.TableDiscoveryAndUploadJob;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static TableDiscoveryAndUploadJob job;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args){
    logger.info("Starting table metadata extractor service");
    if (args.length != 1) {
      System.err.println("Usage: java Main <config-file-path>");
      System.exit(1);
    }

    String configFilePath = args[0];
    Config config = ConfigLoader.loadConfig(configFilePath);

    Injector injector = Guice.createInjector(new RuntimeModule(config));
    job = injector.getInstance(TableDiscoveryAndUploadJob.class);

    Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdownJob));

    job.start();
  }

  private static void shutdownJob() {
    if (job != null) {
      job.shutdown();
    }
  }
}
