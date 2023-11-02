package com.onehouse;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.onehouse.config.Config;
import com.onehouse.config.ConfigLoader;
import com.onehouse.metadataExtractor.TableDiscoveryAndUploadJob;

public class Main {

  private static TableDiscoveryAndUploadJob job;

  public static void main(String[] args) {
    //    if (args.length != 1) {
    //      System.err.println("Usage: java Main <config-file-path>");
    //      System.exit(1);
    //    }

    //    String configFilePath = args[0];
    String configFilePath = "/testConfig.yaml";
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
