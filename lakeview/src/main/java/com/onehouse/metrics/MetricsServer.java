package com.onehouse.metrics;

import static com.onehouse.constants.MetricsConstants.PROMETHEUS_METRICS_SCRAPING_DISABLED;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsServer {
  private final HTTPServer server;

  public MetricsServer(CollectorRegistry registry, int port) {
    if (port != PROMETHEUS_METRICS_SCRAPING_DISABLED) {
      try {
        log.info("Starting metrics server");
        server = initHttpServer(new InetSocketAddress(port), registry);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
      } catch (IOException e) {
        throw new RuntimeException("Failed to start metrics server", e);
      }
    } else {
      server = null;
    }
  }

  static HTTPServer initHttpServer(InetSocketAddress socketAddress, CollectorRegistry registry)
      throws IOException {
    return new HTTPServer(socketAddress, registry);
  }

  public void shutdown() {
    if (server != null) {
      log.info("Shutting down metrics server");
      server.close();
    }
  }
}
