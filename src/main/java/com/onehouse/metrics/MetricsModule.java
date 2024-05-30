package com.onehouse.metrics;

import static com.onehouse.constants.MetricsConstants.PROMETHEUS_METRICS_SCRAPE_PORT;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsModule extends AbstractModule {

  @Provides
  @Singleton
  static Metrics providesMetrics() {
    Metrics metrics = Metrics.getInstance();
    providesMetricsServer(metrics);
    return metrics;
  }

  @Provides
  @Singleton
  static MetricsServer providesMetricsServer(Metrics metrics) {
    return new MetricsServer(metrics.getCollectorRegistry(), PROMETHEUS_METRICS_SCRAPE_PORT);
  }
}
