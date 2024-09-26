package com.onehouse.metrics;

import com.onehouse.config.Config;
import com.onehouse.config.ConfigProvider;
import com.onehouse.constants.MetricsConstants;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.Getter;

public class LakeViewExtractorMetrics {
  private final Metrics metrics;
  private final Metrics.Gauge tablesDiscoveredGaugeMetric;
  private final Config extractorConfig;

  static final String METRICS_COMMON_PREFIX = "lakeView_";

  // Tag keys
  static final String CONFIG_VERSION_TAG_KEY = "config_version";
  static final String EXTRACTOR_JOB_RUN_MODE_TAG_KEY = "extractor_job_run_mode";
  static final String METADATA_UPLOAD_FAILURE_REASON_TAG_KEY = "metadata_upload_failure_reason";

  // Metrics
  static final String TABLE_DISCOVERY_SUCCESS_COUNTER =
      METRICS_COMMON_PREFIX + "table_discovery_success";
  static final String TABLE_DISCOVERY_FAILURE_COUNTER =
      METRICS_COMMON_PREFIX + "table_discovery_failure";
  static final String TABLE_SYNC_SUCCESS_COUNTER = METRICS_COMMON_PREFIX + "table_sync_success";
  static final String TABLE_SYNC_ERROR_COUNTER = METRICS_COMMON_PREFIX + "table_sync_failure";
  static final String TABLE_METADATA_PROCESSING_FAILURE_COUNTER =
      METRICS_COMMON_PREFIX + "table_metadata_processing_failure";

  @Inject
  public LakeViewExtractorMetrics(
      @Nonnull Metrics metrics, @Nonnull ConfigProvider configProvider) {
    this.metrics = metrics;
    this.extractorConfig = configProvider.getConfig();
    this.tablesDiscoveredGaugeMetric =
        metrics.gauge(
            TablesDiscoveredGaugeMetricsMetadata.NAME,
            TablesDiscoveredGaugeMetricsMetadata.DESCRIPTION,
            getDefaultTags());
  }

  public void setDiscoveredTablesPerRound(long numTablesDiscovered) {
    tablesDiscoveredGaugeMetric.setValue(numTablesDiscovered);
    incrementTableDiscoverySuccessCounter();
  }

  private void incrementTableDiscoverySuccessCounter() {
    metrics.increment(TABLE_DISCOVERY_SUCCESS_COUNTER, getDefaultTags());
  }

  public void incrementTableDiscoveryFailureCounter() {
    metrics.increment(TABLE_DISCOVERY_FAILURE_COUNTER, getDefaultTags());
  }

  public void incrementTableSyncSuccessCounter() {
    metrics.increment(TABLE_SYNC_SUCCESS_COUNTER, getDefaultTags());
  }

  public void incrementTableSyncFailureCounter() {
    metrics.increment(TABLE_SYNC_ERROR_COUNTER, getDefaultTags());
  }

  public void incrementTableMetadataProcessingFailureCounter(
      MetricsConstants.MetadataUploadFailureReasons metadataUploadFailureReasons) {
    List<Tag> tags = getDefaultTags();
    tags.add(Tag.of(METADATA_UPLOAD_FAILURE_REASON_TAG_KEY, metadataUploadFailureReasons.name()));
    metrics.increment(TABLE_METADATA_PROCESSING_FAILURE_COUNTER, tags);
  }

  private List<Tag> getDefaultTags() {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of(CONFIG_VERSION_TAG_KEY, extractorConfig.getVersion().toString()));
    tags.add(
        Tag.of(
            EXTRACTOR_JOB_RUN_MODE_TAG_KEY,
            extractorConfig.getMetadataExtractorConfig().getJobRunMode().toString()));
    return tags;
  }

  @Getter
  private static class TablesDiscoveredGaugeMetricsMetadata {
    public static final String NAME = METRICS_COMMON_PREFIX + "discovered_tables";
    public static final String DESCRIPTION = "Number of tables discovered during extractor run";
  }
}
