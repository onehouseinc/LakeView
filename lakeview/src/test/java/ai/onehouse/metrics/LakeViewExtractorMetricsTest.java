package ai.onehouse.metrics;

import static ai.onehouse.metrics.LakeViewExtractorMetrics.CONFIG_VERSION_TAG_KEY;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.EXTRACTOR_JOB_RUN_MODE_TAG_KEY;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.METADATA_UPLOAD_FAILURE_REASON_TAG_KEY;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.METRICS_COMMON_PREFIX;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.TABLE_DISCOVERY_FAILURE_COUNTER;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.TABLE_DISCOVERY_SUCCESS_COUNTER;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.TABLE_METADATA_PROCESSING_FAILURE_COUNTER;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.TABLE_SYNC_ERROR_COUNTER;
import static ai.onehouse.metrics.LakeViewExtractorMetrics.TABLE_SYNC_SUCCESS_COUNTER;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.ConfigVersion;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.constants.MetricsConstants;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LakeViewExtractorMetricsTest {
  @Mock private Metrics metrics;
  @Mock private ConfigProvider configProvider;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Config config;

  @Mock private Metrics.Gauge tablesDiscoveredGaugeMetric;
  @Mock private Metrics.Gauge tablesProcessedGaugeMetric;
  private LakeViewExtractorMetrics hudiMetadataExtractorMetrics;

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
    when(metrics.gauge(
        METRICS_COMMON_PREFIX + "processed_tables",
        "Number of tables processed during extractor run",
        getDefaultTags()))
        .thenReturn(tablesProcessedGaugeMetric);

    hudiMetadataExtractorMetrics = new LakeViewExtractorMetrics(metrics, configProvider);
  }

  @Test
  void testSetDiscoveredTablesPerRound() {
    long numTablesDiscovered = 5L;
    hudiMetadataExtractorMetrics.setDiscoveredTablesPerRound(numTablesDiscovered);

    verify(tablesDiscoveredGaugeMetric).setValue(numTablesDiscovered);
    verify(metrics).increment(TABLE_DISCOVERY_SUCCESS_COUNTER, getDefaultTags());
  }

  @ParameterizedTest
  @EnumSource(value = MetricsConstants.MetadataUploadFailureReasons.class,
          names = {"RATE_LIMITING", "API_FAILURE_USER_ERROR", "UNKNOWN"})
  void testIncrementTableDiscoveryFailureCounter(MetricsConstants.MetadataUploadFailureReasons reason) {
    hudiMetadataExtractorMetrics.incrementTableDiscoveryFailureCounter(reason);
    List<Tag> tags = getDefaultTags();
    tags.add(Tag.of(TABLE_DISCOVERY_FAILURE_COUNTER, reason.name()));
    verify(metrics).increment(TABLE_DISCOVERY_FAILURE_COUNTER, tags);
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

  @Test
  void testResetTableProcessedGauge() {
    hudiMetadataExtractorMetrics.resetTableProcessedGauge();
    verify(tablesProcessedGaugeMetric).setValue(0L);
  }

  @Test
  void testIncrementTablesProcessedCounter() {
    hudiMetadataExtractorMetrics.incrementTablesProcessedCounter();
    verify(tablesProcessedGaugeMetric).increment();
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
