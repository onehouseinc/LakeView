package com.onehouse.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsServerTest {

  private CollectorRegistry registry;
  private int port;

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

      assertEquals("Failed to start Prometheus server", exception.getMessage());
    }
  }
}
