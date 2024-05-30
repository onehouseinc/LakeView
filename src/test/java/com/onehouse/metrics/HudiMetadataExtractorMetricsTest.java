package com.onehouse.metrics;

import static com.onehouse.metrics.HudiMetadataExtractorMetrics.CONFIG_VERSION_TAG_KEY;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.EXTRACTOR_JOB_RUN_MODE_TAG_KEY;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.METADATA_UPLOAD_FAILURE_REASON_TAG_KEY;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.METRICS_COMMON_PREFIX;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.TABLE_DISCOVERY_FAILURE_COUNTER;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.TABLE_DISCOVERY_SUCCESS_COUNTER;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.TABLE_METADATA_PROCESSING_FAILURE_COUNTER;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.TABLE_SYNC_ERROR_COUNTER;
import static com.onehouse.metrics.HudiMetadataExtractorMetrics.TABLE_SYNC_SUCCESS_COUNTER;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.onehouse.config.Config;
import com.onehouse.config.ConfigProvider;
import com.onehouse.config.ConfigVersion;
import com.onehouse.config.models.configv1.MetadataExtractorConfig;
import com.onehouse.constants.MetricsConstants;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HudiMetadataExtractorMetricsTest {
  @Mock private Metrics metrics;
  @Mock private ConfigProvider configProvider;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Config config;

  @Mock private Metrics.Gauge tablesDiscoveredGaugeMetric;
  private HudiMetadataExtractorMetrics hudiMetadataExtractorMetrics;

  @BeforeEach
  void setUp() {
    when(configProvider.getConfig()).thenReturn(config);
    when(config.getVersion()).thenReturn(ConfigVersion.V1);
    when(config.getMetadataExtractorConfig().getJobRunMode())
        .thenReturn(MetadataExtractorConfig.JobRunMode.CONTINUOUS);
    when(metrics.gauge(
            METRICS_COMMON_PREFIX + "discovered_tables",
            "Number of tables discovered during extractor run",
            getDefaultTags()))
        .thenReturn(tablesDiscoveredGaugeMetric);

    hudiMetadataExtractorMetrics = new HudiMetadataExtractorMetrics(metrics, configProvider);
  }

  @Test
  void testSetDiscoveredTablesPerRound() {
    long numTablesDiscovered = 5L;
    hudiMetadataExtractorMetrics.setDiscoveredTablesPerRound(numTablesDiscovered);

    verify(tablesDiscoveredGaugeMetric).setValue(numTablesDiscovered);
    verify(metrics).increment(TABLE_DISCOVERY_SUCCESS_COUNTER, getDefaultTags());
  }

  @Test
  void testIncrementTableDiscoveryFailureCounter() {
    hudiMetadataExtractorMetrics.incrementTableDiscoveryFailureCounter();

    verify(metrics).increment(TABLE_DISCOVERY_FAILURE_COUNTER, getDefaultTags());
  }

  @Test
  void testIncrementTableSyncSuccessCounter() {
    hudiMetadataExtractorMetrics.incrementTableSyncSuccessCounter();

    verify(metrics).increment(TABLE_SYNC_SUCCESS_COUNTER, getDefaultTags());
  }

  @Test
  void testIncrementTableSyncFailureCounter() {
    hudiMetadataExtractorMetrics.incrementTableSyncFailureCounter();

    verify(metrics).increment(TABLE_SYNC_ERROR_COUNTER, getDefaultTags());
  }

  @Test
  void testIncrementTableMetadataUploadFailureCounter() {
    MetricsConstants.MetadataUploadFailureReasons reason =
        MetricsConstants.MetadataUploadFailureReasons.UNKNOWN;
    hudiMetadataExtractorMetrics.incrementTableMetadataProcessingFailureCounter(reason);
    List<Tag> tags = getDefaultTags();
    tags.add(Tag.of(METADATA_UPLOAD_FAILURE_REASON_TAG_KEY, reason.name()));
    verify(metrics).increment(TABLE_METADATA_PROCESSING_FAILURE_COUNTER, tags);
  }

  private List<Tag> getDefaultTags() {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of(CONFIG_VERSION_TAG_KEY, ConfigVersion.V1.toString()));
    tags.add(
        Tag.of(
            EXTRACTOR_JOB_RUN_MODE_TAG_KEY,
            MetadataExtractorConfig.JobRunMode.CONTINUOUS.toString()));
    return tags;
  }
}
