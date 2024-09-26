package ai.onehouse.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsTest {

  private Metrics metrics;
  @Mock private PrometheusMeterRegistry meterRegistry;
  private Map<String, Metrics.Gauge> gaugeMap;

  @BeforeEach
  void setUp() {
    gaugeMap = new HashMap<>();
    metrics = Mockito.spy(new Metrics(meterRegistry, gaugeMap));
  }

  @Test
  void testGetInstance() {
    try (MockedStatic<Metrics> mockedMetrics =
        Mockito.mockStatic(Metrics.class, Mockito.CALLS_REAL_METHODS)) {
      mockedMetrics.when(Metrics::getInstance).thenReturn(metrics);
      Metrics instance = Metrics.getInstance();
      assertNotNull(instance);
      assertEquals(metrics, instance);
    }
  }

  @Test
  void testGetCollectorRegistry() {
    CollectorRegistry registry = mock(CollectorRegistry.class);
    when(meterRegistry.getPrometheusRegistry()).thenReturn(registry);

    CollectorRegistry result = metrics.getCollectorRegistry();
    assertNotNull(result);
    assertEquals(registry, result);
  }

  @Test
  void testIncrement() {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of("key1", "value1"));
    tags.add(Tag.of("key2", "value2"));

    List<String> tagsString = new ArrayList<>();
    for (Tag tag : tags) {
      tagsString.add(tag.getKey());
      tagsString.add(tag.getValue());
    }
    doNothing().when(metrics).createAndIncrementCounter("test.counter", tagsString);
    metrics.increment("test.counter", tags);
    verify(metrics, times(1)).createAndIncrementCounter(eq("test.counter"), anyList());
  }

  @Test
  void testGauge() {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of("key1", "value1"));
    tags.add(Tag.of("key2", "value2"));

    Meter.Id meterId = mock(Meter.Id.class);
    doReturn(meterId).when(metrics).getGaugeRegisterId(anyString(), anyString(), any(), anyList());
    Metrics.Gauge gauge = metrics.gauge("test.gauge", "A test gauge", tags);
    assertNotNull(gauge);
    assertEquals(
        String.format(
            "Metrics.Gauge(value=0, meterId=Mock for Id, hashCode: %s)", meterId.hashCode()),
        gauge.toString());
    assertEquals(meterId, gauge.getMeterId());
    assertEquals(0, gauge.get().intValue());
    gauge.setValue(10);
    assertEquals(10, gauge.get().intValue());

    Metrics.Gauge sameGauge = metrics.gauge("test.gauge", "A test gauge", tags);
    assertEquals(gauge, sameGauge);
  }

  @Test
  void testGaugeFailure() {
    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of("key1", "value1"));
    tags.add(Tag.of("key2", "value2"));

    Meter.Id meterId = mock(Meter.Id.class);
    doReturn(meterId).when(metrics).getGaugeRegisterId(anyString(), anyString(), any(), anyList());
    Metrics.Gauge gauge = metrics.gauge("test.gauge", "A test gauge", tags);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> gauge.setMeterId(meterId));
    assertEquals("MeterId cannot be set more than once", exception.getMessage());
  }
}
