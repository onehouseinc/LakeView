package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.PROCESS_TABLE_METADATA_SYNC_DURATION_SECONDS;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static org.mockito.Mockito.*;

import com.onehouse.config.Config;
import com.onehouse.metadata_extractor.models.Table;
import com.onehouse.metrics.HudiMetadataExtractorMetrics;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableDiscoveryAndUploadJobTest {

  @Mock private TableDiscoveryService mockTableDiscoveryService;

  @Mock private TableMetadataUploaderService mockTableMetadataUploaderService;

  @Mock private ScheduledExecutorService mockScheduler;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Config config;

  @Mock private HudiMetadataExtractorMetrics mockHudiMetadataExtractorMetrics;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private TableDiscoveryAndUploadJob job;

  @BeforeEach
  void setUp() {
    job =
        new TableDiscoveryAndUploadJob(
            mockTableDiscoveryService,
            mockTableMetadataUploaderService,
            mockHudiMetadataExtractorMetrics) {
          @Override
          ScheduledExecutorService getScheduler() {
            return mockScheduler;
          }
        };
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
          .thenReturn(failedFuture(new Exception("error")));
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
      verify(mockHudiMetadataExtractorMetrics).incrementTableDiscoveryFailureCounter();
    } else {
      verify(mockTableMetadataUploaderService, times(1))
          .uploadInstantsInTables(Collections.singleton(discoveredTable));
      verify(mockHudiMetadataExtractorMetrics).setDiscoveredTablesPerRound(1);
      if (tableMetadataExtractionFailed) {
        verify(mockHudiMetadataExtractorMetrics).incrementTableSyncFailureCounter();
      } else {
        verify(mockHudiMetadataExtractorMetrics).incrementTableSyncSuccessCounter();
      }
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

  @Test
  void testShutdown() {
    job.shutdown();
    verify(mockScheduler).shutdown();
  }

  public static <R> CompletableFuture<R> failedFuture(Throwable error) {
    CompletableFuture<R> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
