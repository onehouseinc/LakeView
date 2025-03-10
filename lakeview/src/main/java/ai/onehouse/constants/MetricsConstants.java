package ai.onehouse.constants;

public class MetricsConstants {
  public static final int PROMETHEUS_METRICS_SCRAPING_DISABLED = 0;
  public static final int PROMETHEUS_METRICS_SCRAPE_PORT =
      Integer.parseInt(
          System.getenv()
              .getOrDefault(
                  "PROMETHEUS_METRICS_SCRAPE_PORT",
                  String.valueOf(PROMETHEUS_METRICS_SCRAPING_DISABLED)));

  public enum MetadataUploadFailureReasons {
    API_FAILURE_USER_ERROR,
    API_FAILURE_SYSTEM_ERROR,
    HOODIE_PROPERTY_NOT_FOUND_OR_CORRUPTED,
    PRESIGNED_URL_UPLOAD_FAILURE,
    RATE_LIMITING,
    NO_TABLES_TO_INITIALIZE,
    UNKNOWN,
  }
}
