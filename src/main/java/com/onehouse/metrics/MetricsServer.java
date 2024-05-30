package com.onehouse.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsServer {
  public MetricsServer(CollectorRegistry registry, int port) {
    try (HTTPServer server = initHttpServer(new InetSocketAddress(port), registry)) {
      log.info("Starting metrics server");
      Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Prometheus server", e);
    }
  }

  static HTTPServer initHttpServer(InetSocketAddress socketAddress, CollectorRegistry registry)
      throws IOException {
    return new HTTPServer(socketAddress, registry);
  }
}
