package com.onehouse.constants;

public class MetricsConstants {
  public static final int PROMETHEUS_METRICS_SCRAPE_PORT =
      Math.min(
          7070,
          Integer.parseInt(System.getenv().getOrDefault("PROMETHEUS_METRICS_SCRAPE_PORT", "7070")));
  public static final String METRICS_COMMON_PREFIX = "hudi_metadata_extractor_";

  // Tag keys
  public static final String CONFIG_VERSION_TAG_KEY = "config_version";
  public static final String EXTRACTOR_JOB_RUN_MODE_TAG_KEY = "config_version";

  // Metrics
  public static final String TABLE_DISCOVERY_SUCCESS_COUNTER =
      METRICS_COMMON_PREFIX + "table_discovery_success"; // number of successful table discovery rounds
  public static final String TABLE_DISCOVERY_ERROR_COUNTER =
      METRICS_COMMON_PREFIX + "table_discovery_failure"; // number of unrecoverable failures
}
