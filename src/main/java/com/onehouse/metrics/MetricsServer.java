package com.onehouse.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.net.InetSocketAddress;

public class MetricsServer {
  public MetricsServer(CollectorRegistry registry, int port) {
    try (HTTPServer server = new HTTPServer(new InetSocketAddress(port), registry)) {
      Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Prometheus server", e);
    }
  }
}
