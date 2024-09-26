package com.onehouse.metrics;

import static io.micrometer.prometheus.PrometheusConfig.DEFAULT;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class Metrics {
  private final PrometheusMeterRegistry meterRegistry;
  private static final Metrics INSTANCE =
      new Metrics(new PrometheusMeterRegistry(DEFAULT), new HashMap<>());

  private Map<String, Gauge> gaugeMap;

  public static Metrics getInstance() {
    return INSTANCE;
  }

  public CollectorRegistry getCollectorRegistry() {
    return meterRegistry.getPrometheusRegistry();
  }

  public void increment(String name, List<Tag> tags) {
    List<String> tagList = new ArrayList<>();
    for (Tag tag : tags) {
      tagList.add(tag.getKey());
      tagList.add(tag.getValue());
    }
    createAndIncrementCounter(name, tagList);
  }

  public Gauge gauge(String name, String description, List<Tag> tags) {
    String gaugeKey = generateGaugeKey(name, description, tags);
    Gauge gauge = gaugeMap.get(gaugeKey);
    if (gauge != null) {
      return gauge;
    }

    gauge = new Gauge();

    gauge.setMeterId(getGaugeRegisterId(name, description, gauge, tags));
    gaugeMap.put(gaugeKey, gauge);
    return gauge;
  }

  Meter.Id getGaugeRegisterId(String name, String description, Gauge gauge, List<Tag> tags) {
    return io.micrometer.core.instrument.Gauge.builder(name, gauge)
        .tags(tags)
        .description(description)
        .register(meterRegistry)
        .getId();
  }

  void createAndIncrementCounter(String name, List<String> tagList) {
    Counter.builder(name).tags(tagList.toArray(new String[0])).register(meterRegistry).increment();
  }

  // Generates a unique key based on the name, description, and tags
  private String generateGaugeKey(String name, String description, List<Tag> tags) {
    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(name);
    keyBuilder.append("-");
    keyBuilder.append(description);
    keyBuilder.append("-");
    for (Tag tag : tags) {
      keyBuilder.append(tag.getKey());
      keyBuilder.append(":");
      keyBuilder.append(tag.getValue());
      keyBuilder.append("-");
    }
    return keyBuilder.toString();
  }

  @EqualsAndHashCode
  @ToString
  public static class Gauge implements Supplier<Number> {
    private final AtomicLong value = new AtomicLong(0);
    @Getter private Meter.Id meterId;

    public void setValue(long val) {
      value.set(val);
    }

    public void setMeterId(Meter.Id id) {
      Preconditions.checkArgument(this.meterId == null, "MeterId cannot be set more than once");
      this.meterId = id;
    }

    @Override
    public Number get() {
      return value.get();
    }
  }
}
