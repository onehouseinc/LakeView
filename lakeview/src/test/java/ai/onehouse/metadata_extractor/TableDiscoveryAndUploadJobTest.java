package ai.onehouse.metadata_extractor;

import static ai.onehouse.constants.MetadataExtractorConstants.PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;
import static ai.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static org.mockito.Mockito.*;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.constants.MetricsConstants;
import ai.onehouse.exceptions.RateLimitException;
import ai.onehouse.metadata_extractor.models.Table;
import ai.onehouse.metrics.LakeViewExtractorMetrics;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import ai.onehouse.storage.AsyncStorageClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableDiscoveryAndUploadJobTest {

  @Mock private TableDiscoveryService mockTableDiscoveryService;

  @Mock private TableMetadataUploaderService mockTableMetadataUploaderService;

  @Mock private ScheduledExecutorService mockScheduler;

  @Mock private AsyncStorageClient asyncStorageClient;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Config config;

  @Mock private LakeViewExtractorMetrics mockHudiMetadataExtractorMetrics;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private TableDiscoveryAndUploadJob job;

  @BeforeEach
  void setUp(TestInfo info) {
    Instant fixedInstant =
        info.getDisplayName().startsWith("2023") ? Instant.parse(info.getDisplayName()) : Instant.now();
    try (MockedStatic<Instant> mockedInstant =
             mockStatic(Instant.class, Answers.CALLS_REAL_METHODS)) {
      mockedInstant.when(Instant::now).thenReturn(fixedInstant);
      job =
          new TableDiscoveryAndUploadJob(
              mockTableDiscoveryService,
              mockTableMetadataUploaderService,
              mockHudiMetadataExtractorMetrics,
              asyncStorageClient) {
            @Override
            ScheduledExecutorService getScheduler() {
              return mockScheduler;
            }
          };
    }
  }

  private static Stream<Arguments> continuousModeFailureCases() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false));
  }

  @ParameterizedTest
  @MethodSource("continuousModeFailureCases")
  void testRunInContinuousMode(boolean discoveryFailed, boolean tableMetadataExtractionFailed) {
    Table discoveredTable =
        Table.builder()
            .absoluteTableUri("absolute_uri")
            .lakeName("lake")
            .databaseName("database")
            .build();
    if (discoveryFailed) {
      when(mockTableDiscoveryService.discoverTables())
          .thenReturn(failedFuture(new RateLimitException("error")));
    } else {
      when(mockTableDiscoveryService.discoverTables())
          .thenReturn(CompletableFuture.completedFuture(Collections.singleton(discoveredTable)));

      // If discovery fails, table upload is never invoked
      if (tableMetadataExtractionFailed) {
        when(mockTableMetadataUploaderService.uploadInstantsInTables(
                Collections.singleton(discoveredTable)))
            .thenReturn(failedFuture(new Exception("error")));
      } else {
        when(mockTableMetadataUploaderService.uploadInstantsInTables(
                Collections.singleton(discoveredTable)))
            .thenReturn(CompletableFuture.completedFuture(null));
      }
    }

    when(config.getMetadataExtractorConfig().getTableDiscoveryIntervalMinutes())
        .thenReturn(TABLE_DISCOVERY_INTERVAL_MINUTES);
    when(config.getMetadataExtractorConfig().getProcessTableMetadataSyncDurationSeconds())
        .thenReturn(PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS);
    job.runInContinuousMode(config);

    verify(mockScheduler)
        .scheduleAtFixedRate(
            runnableCaptor.capture(),
            eq(0L),
            eq((long) TABLE_DISCOVERY_INTERVAL_MINUTES),
            eq(TimeUnit.MINUTES));
    verify(mockScheduler)
        .scheduleAtFixedRate(
            runnableCaptor.capture(),
            eq(0L),
            eq((long) PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS),
            eq(TimeUnit.SECONDS));

    Runnable discoveryTask = runnableCaptor.getAllValues().get(0);
    Runnable uploadTask = runnableCaptor.getAllValues().get(1);
    discoveryTask.run();
    uploadTask.run();

    verify(mockTableDiscoveryService, times(1)).discoverTables();

    if (discoveryFailed) {
      verify(mockHudiMetadataExtractorMetrics)
          .incrementTableDiscoveryFailureCounter(MetricsConstants.MetadataUploadFailureReasons.RATE_LIMITING);
    } else {
      verify(mockTableMetadataUploaderService, times(1))
          .uploadInstantsInTables(Collections.singleton(discoveredTable));
      verify(mockHudiMetadataExtractorMetrics).setDiscoveredTablesPerRound(1);
      if (tableMetadataExtractionFailed) {
        verify(mockHudiMetadataExtractorMetrics).incrementTableSyncFailureCounter();
      } else {
        verify(mockHudiMetadataExtractorMetrics).incrementTableSyncSuccessCounter();
      }
      verify(mockHudiMetadataExtractorMetrics, times(1)).resetTableProcessedGauge();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRunOnce(boolean isSucceeded) {
    Table discoveredTable =
        Table.builder()
            .absoluteTableUri("absolute_uri")
            .lakeName("lake")
            .databaseName("database")
            .build();
    when(mockTableDiscoveryService.discoverTables())
        .thenReturn(CompletableFuture.completedFuture(Collections.singleton(discoveredTable)));
    when(mockTableMetadataUploaderService.uploadInstantsInTables(
            Collections.singleton(discoveredTable)))
        .thenReturn(CompletableFuture.completedFuture(isSucceeded));
    job.runOnce();
    verify(mockTableDiscoveryService, times(1)).discoverTables();
    verify(mockTableMetadataUploaderService, times(1))
        .uploadInstantsInTables(Collections.singleton(discoveredTable));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTestCases")
  void testRunOnceWithRetry(String timeString, boolean isSucceeded, int numTries) {
    Table discoveredTable =
        Table.builder()
            .absoluteTableUri("absolute_uri")
            .lakeName("lake")
            .databaseName("database")
            .build();
    when(mockTableDiscoveryService.discoverTables())
        .thenReturn(CompletableFuture.completedFuture(Collections.singleton(discoveredTable)));
    when(mockTableMetadataUploaderService.uploadInstantsInTables(
        Collections.singleton(discoveredTable)))
        .thenReturn(CompletableFuture.completedFuture(isSucceeded));
    when(config.getMetadataExtractorConfig().getJobRunMode())
        .thenReturn(MetadataExtractorConfig.JobRunMode.ONCE_WITH_RETRY);
    if (!isSucceeded) {
      when(config.getMetadataExtractorConfig().getCronScheduleForPullModel())
          .thenReturn("0 */1 * * *");
      if (numTries > 1) {
        when(config.getMetadataExtractorConfig().getMaxRunCountForPullModel())
            .thenReturn(5);
      }
    }
    job.runOnce(config);
    verify(mockTableDiscoveryService, times(numTries))
        .discoverTables();
    verify(mockTableMetadataUploaderService, times(numTries))
        .uploadInstantsInTables(Collections.singleton(discoveredTable));
  }

  private static Stream<Arguments> generateTestCases() {
    return Stream.of(
        Arguments.of("2023-10-01T00:42:00Z", true, 1),
        Arguments.of("2023-10-01T00:42:00Z", false, 5),
        Arguments.of("2023-10-01T00:52:00Z", false, 1));
  }

  @Test
  void testShutdown() {
    job.shutdown();
    verify(mockScheduler).shutdown();
  }

  @Test
  void testShouldRunAgainForRunOnceConfiguration() {
    when(config.getMetadataExtractorConfig().getCronScheduleForPullModel()).thenReturn("0 */6 * * *");
    job.shouldRunAgainForRunOnceConfiguration(config);
  }

  public static <R> CompletableFuture<R> failedFuture(Throwable error) {
    CompletableFuture<R> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
