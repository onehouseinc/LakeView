package com.onehouse.metadata_extractor;

import static com.onehouse.constants.MetadataExtractorConstants.TABLE_DISCOVERY_INTERVAL_MINUTES;
import static com.onehouse.constants.MetadataExtractorConstants.TABLE_METADATA_UPLOAD_INTERVAL_MINUTES;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import com.onehouse.metadata_extractor.TableDiscoveryService;
import com.onehouse.metadata_extractor.TableMetadataUploaderService;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.onehouse.metadata_extractor.models.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MockitoExtension.class)
public class TableDiscoveryAndUploadJobTest {

    @Mock
    private TableDiscoveryService mockTableDiscoveryService;

    @Mock
    private TableMetadataUploaderService mockTableMetadataUploaderService;

    @Mock
    private ScheduledExecutorService mockScheduler;

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    private TableDiscoveryAndUploadJob job;

    @BeforeEach
    public void setUp() {
        job = new TableDiscoveryAndUploadJob(mockTableDiscoveryService, mockTableMetadataUploaderService) {
            @Override
            ScheduledExecutorService getScheduler() {
                return mockScheduler;
            }
        };
    }

    @Test
    public void testRunInContinuousMode() {
        Table discoveredTable = Table.builder().absoluteTableUri("absolute_uri").lakeName("lake").databaseName("database").build();
        when(mockTableDiscoveryService.discoverTables()).thenReturn(CompletableFuture.completedFuture(Set.of(discoveredTable)));
        when(mockTableMetadataUploaderService.uploadInstantsInTables(Set.of(discoveredTable))).thenReturn(CompletableFuture.completedFuture(null));

        job.runInContinuousMode();

        verify(mockScheduler).scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), eq((long) TABLE_DISCOVERY_INTERVAL_MINUTES), eq(TimeUnit.MINUTES));
        verify(mockScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), eq(0L), eq((long)TABLE_METADATA_UPLOAD_INTERVAL_MINUTES), eq(TimeUnit.MINUTES));


        Runnable discoveryTask = runnableCaptor.getAllValues().get(0);
        Runnable uploadTask = runnableCaptor.getAllValues().get(1);
        discoveryTask.run();
        uploadTask.run();

        verify(mockTableDiscoveryService, times(1)).discoverTables();
        verify(mockTableMetadataUploaderService, times(1)).uploadInstantsInTables(Set.of(discoveredTable));

    }

    @Test
    public void testRunOnce() {
        Table discoveredTable = Table.builder().absoluteTableUri("absolute_uri").lakeName("lake").databaseName("database").build();
        when(mockTableDiscoveryService.discoverTables()).thenReturn(CompletableFuture.completedFuture(Set.of(discoveredTable)));
        when(mockTableMetadataUploaderService.uploadInstantsInTables(Set.of(discoveredTable))).thenReturn(CompletableFuture.completedFuture(null));
        job.runOnce();
        verify(mockTableDiscoveryService, times(1)).discoverTables();
        verify(mockTableMetadataUploaderService, times(1)).uploadInstantsInTables(Set.of(discoveredTable));
    }

    @Test
    public void testShutdown() {
        job.shutdown();
        verify(mockScheduler).shutdown();
    }

}
