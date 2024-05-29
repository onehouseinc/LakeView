package com.onehouse.constants;

public class MetricsConstants {
  public static final int PROMETHEUS_METRICS_SCRAPE_PORT =
      Math.min(
          7070,
          Integer.parseInt(System.getenv().getOrDefault("PROMETHEUS_METRICS_SCRAPE_PORT", "7070")));
  public static final String METRICS_COMMON_PREFIX = "hudi_metadata_extractor_";

  // Tag keys
  public static final String CONFIG_VERSION_TAG_KEY = "config_version";
  public static final String EXTRACTOR_JOB_RUN_MODE_TAG_KEY = "extractor_job_run_mode";
  public static final String METADATA_UPLOAD_FAILURE_REASON_TAG_KEY =
      "metadata_upload_failure_reason";

  // Metrics
  public static final String TABLE_DISCOVERY_SUCCESS_COUNTER =
      METRICS_COMMON_PREFIX
          + "table_discovery_success"; // number of successful table discovery rounds
  public static final String TABLE_DISCOVERY_FAILURE_COUNTER =
      METRICS_COMMON_PREFIX + "table_discovery_failure"; // number of unrecoverable failures
  public static final String TABLE_SYNC_SUCCESS_COUNTER =
      METRICS_COMMON_PREFIX + "table_sync_success"; // number of successful table sync rounds
  public static final String TABLE_SYNC_ERROR_COUNTER =
      METRICS_COMMON_PREFIX + "table_sync_failure"; // number of unrecoverable failures
  public static final String TABLE_METADATA_PROCESSING_FAILURE_COUNTER =
      METRICS_COMMON_PREFIX
          + "table_metadata_processing_failure"; // number of errors when processing table metadata

  public enum MetadataUploadFailureReasons {
    API_FAILURE_USER_ERROR,
    API_FAILURE_SYSTEM_ERROR,
    HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED,
    PRESIGNED_URL_UPLOAD_FAILURE,
    UNKNOWN,
  }
}
