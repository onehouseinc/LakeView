package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static org.mockito.Mockito.*;

import com.onehouse.metadata_extractor.models.Table;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TableDiscoveryAndUploadJobTest {

  @Mock private TableDiscoveryService mockTableDiscoveryService;

  @Mock private TableMetadataUploaderService mockTableMetadataUploaderService;

  @Mock private ScheduledExecutorService mockScheduler;

  @Captor private ArgumentCaptor<Runnable> runnableCaptor;

  private TableDiscoveryAndUploadJob job;

  @BeforeEach
  void setUp() {
    job =
        new TableDiscoveryAndUploadJob(
            mockTableDiscoveryService, mockTableMetadataUploaderService) {
          @Override
          ScheduledExecutorService getScheduler() {
            return mockScheduler;
          }
        };
  }

  @Test
  void testRunInContinuousMode() {
    Table discoveredTable =
        Table.builder()
            .absoluteTableUri("absolute_uri")
            .lakeName("lake")
            .databaseName("database")
            .build();
    when(mockTableDiscoveryService.discoverTables())
        .thenReturn(CompletableFuture.completedFuture(Set.of(discoveredTable)));
    when(mockTableMetadataUploaderService.uploadInstantsInTables(Set.of(discoveredTable)))
        .thenReturn(CompletableFuture.completedFuture(null));

    job.runInContinuousMode();

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
            eq((long) job.getProcessTableMetadataSyncDurationSeconds()),
            eq(TimeUnit.SECONDS));

    Runnable discoveryTask = runnableCaptor.getAllValues().get(0);
    Runnable uploadTask = runnableCaptor.getAllValues().get(1);
    discoveryTask.run();
    uploadTask.run();

    verify(mockTableDiscoveryService, times(1)).discoverTables();
    verify(mockTableMetadataUploaderService, times(1))
        .uploadInstantsInTables(Set.of(discoveredTable));
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
        .thenReturn(CompletableFuture.completedFuture(Set.of(discoveredTable)));
    when(mockTableMetadataUploaderService.uploadInstantsInTables(Set.of(discoveredTable)))
        .thenReturn(CompletableFuture.completedFuture(isSucceeded));
    job.runOnce();
    verify(mockTableDiscoveryService, times(1)).discoverTables();
    verify(mockTableMetadataUploaderService, times(1))
        .uploadInstantsInTables(Set.of(discoveredTable));
  }

  @Test
  void testShutdown() {
    job.shutdown();
    verify(mockScheduler).shutdown();
  }
}
