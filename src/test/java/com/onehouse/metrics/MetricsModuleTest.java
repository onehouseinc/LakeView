package com.onehouse.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MetricsModuleTest {
  @Mock private Metrics metrics;

  @Test
  void testProvidesMetrics() {
    Metrics providedMetrics = MetricsModule.providesMetrics();
    assertNotNull(providedMetrics, "Metrics instance should not be null");
  }

  @Test
  void testProvidesMetricsServer() {
    when(metrics.getCollectorRegistry()).thenReturn(new CollectorRegistry());

    MetricsServer metricsServer = MetricsModule.providesMetricsServer(metrics);
    assertNotNull(metricsServer, "MetricsServer instance should not be null");
    verify(metrics).getCollectorRegistry();
  }
}
