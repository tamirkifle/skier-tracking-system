package skiers.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConsumerMetricsTest {

  @Test
  @DisplayName("write outcomes are counted separately")
  void countsOutcomesSeparately() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ConsumerMetrics metrics = new ConsumerMetrics(registry);

    metrics.recordWritten(25, 1_000_000L);
    metrics.recordRetry();
    metrics.recordDropped();
    metrics.recordRequeued();

    assertThat(metrics.writtenCount()).isEqualTo(25);
    assertThat(metrics.retryCount()).isEqualTo(1);
    assertThat(metrics.droppedCount()).isEqualTo(1);
    assertThat(registry.get("skier.write.batch.size").summary().mean()).isEqualTo(25.0);
  }
}
