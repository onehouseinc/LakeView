package com.onehouse.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsModuleTest {
  @Mock private Metrics metrics;

  @Test
  void testProvidesMetrics() {
    try (MockedStatic<Metrics> mockedStatic = mockStatic(Metrics.class)) {
      mockedStatic.when(() -> Metrics.getInstance()).thenReturn(metrics);
      when(metrics.getCollectorRegistry()).thenReturn(new CollectorRegistry());
      Metrics providedMetrics = MetricsModule.providesMetrics();
      assertNotNull(providedMetrics, "Metrics instance should not be null");
    }
  }
}
