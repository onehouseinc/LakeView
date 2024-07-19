package com.onehouse.metrics;

import static com.onehouse.constants.MetricsConstants.PROMETHEUS_METRICS_SCRAPING_DISABLED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsServerTest {

  private CollectorRegistry registry;
  private int port;
  @Mock HTTPServer httpServer;

  @BeforeEach
  void setUp() {
    registry = new CollectorRegistry();
    port = 1234; // example port
  }

  @Test
  @SneakyThrows
  void testMetricsServerFailure() {
    try (MockedStatic<MetricsServer> mocked = mockStatic(MetricsServer.class)) {
      mocked
          .when(() -> MetricsServer.initHttpServer(any(), any()))
          .thenThrow(new IOException("exception"));
      RuntimeException exception =
          assertThrows(
              RuntimeException.class,
              () -> {
                new MetricsServer(registry, port);
              });

      assertEquals("Failed to start metrics server", exception.getMessage());
    }
  }

  @Test
  @SneakyThrows
  void testMetricsServerShutdown() {
    try (MockedStatic<MetricsServer> mocked = mockStatic(MetricsServer.class)) {
      mocked.when(() -> MetricsServer.initHttpServer(any(), any())).thenReturn(httpServer);
      MetricsServer metricsServer = new MetricsServer(registry, port);
      metricsServer.shutdown();

      verify(httpServer).close();
    }
  }

  @Test
  @SneakyThrows
  void testMetricsServerInitWhenScrapingIsNotConfigured() {
    try (MockedStatic<MetricsServer> mocked = mockStatic(MetricsServer.class)) {
      MetricsServer metricsServer =
          new MetricsServer(registry, PROMETHEUS_METRICS_SCRAPING_DISABLED);
      metricsServer.shutdown();
      mocked.verify(() -> MetricsServer.initHttpServer(any(), any()), times(0));
    }
  }
}
