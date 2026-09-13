package skiers.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class IngestMetricsConfigTest {

  private static final Set<String> FRAMEWORK_OWNED_METERS = Set.of("http.server.requests");

  @Test
  void everyIngestPercentilesHistogramPropertyNamesARegisteredMeter() throws IOException {
    Set<String> configuredMeterNames = readSkierPercentilesHistogramMeterNames();
    Set<String> registeredMeterNames = registerIngestMetricsAndCollectNames();

    assertThat(configuredMeterNames)
        .as(
            "management.metrics.distribution.percentiles-histogram.* properties in"
                + " application.properties must name a meter IngestMetrics actually registers")
        .allMatch(registeredMeterNames::contains);
  }

  private Set<String> readSkierPercentilesHistogramMeterNames() throws IOException {
    Properties properties = new Properties();
    try (InputStream in = getClass().getResourceAsStream("/application.properties")) {
      properties.load(in);
    }
    String prefix = "management.metrics.distribution.percentiles-histogram.";
    return properties.stringPropertyNames().stream()
        .filter(key -> key.startsWith(prefix))
        .map(key -> key.substring(prefix.length()))
        .filter(meterName -> !FRAMEWORK_OWNED_METERS.contains(meterName))
        .collect(Collectors.toSet());
  }

  private Set<String> registerIngestMetricsAndCollectNames() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new IngestMetrics(registry);
    return registry.getMeters().stream()
        .map(Meter::getId)
        .map(Meter.Id::getName)
        .collect(Collectors.toSet());
  }
}
