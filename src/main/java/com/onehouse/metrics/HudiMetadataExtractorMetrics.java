package com.onehouse.metrics;

import static com.onehouse.constants.MetricsConstants.CONFIG_VERSION_TAG_KEY;
import static com.onehouse.constants.MetricsConstants.EXTRACTOR_JOB_RUN_MODE_TAG_KEY;
import static com.onehouse.constants.MetricsConstants.METADATA_UPLOAD_FAILURE_REASON_TAG_KEY;
import static com.onehouse.constants.MetricsConstants.METRICS_COMMON_PREFIX;
import static com.onehouse.constants.MetricsConstants.TABLE_DISCOVERY_FAILURE_COUNTER;
import static com.onehouse.constants.MetricsConstants.TABLE_DISCOVERY_SUCCESS_COUNTER;
import static com.onehouse.constants.MetricsConstants.TABLE_METADATA_PROCESSING_FAILURE_COUNTER;
import static com.onehouse.constants.MetricsConstants.TABLE_SYNC_ERROR_COUNTER;
import static com.onehouse.constants.MetricsConstants.TABLE_SYNC_SUCCESS_COUNTER;

import com.onehouse.config.Config;
import com.onehouse.config.ConfigProvider;
import com.onehouse.constants.MetricsConstants;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.Getter;

public class HudiMetadataExtractorMetrics {
  private final Metrics metrics;
  private final Metrics.Gauge tablesDiscoveredGaugeMetric;
  private final Config extractorConfig;

  @Inject
  public HudiMetadataExtractorMetrics(
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
