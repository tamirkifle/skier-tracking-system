package skiers.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConsumerMetricsTest {

  @Test
  @DisplayName("a bound depth gauge keeps reporting after the caller's reference is gone")
  void boundGaugeSurvivesGarbageCollection() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ConsumerMetrics metrics = new ConsumerMetrics(registry);

    AtomicInteger source = new AtomicInteger(7);
    metrics.bindQueueDepth(registry, "test.depth", source::get);

    // Micrometer holds gauge sources weakly, so an unreferenced supplier reads NaN.
    System.gc();
    System.gc();

    assertThat(registry.get("test.depth").gauge().value()).isEqualTo(7.0);

    source.set(42);
    assertThat(registry.get("test.depth").gauge().value()).isEqualTo(42.0);
  }

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

  @Test
  @DisplayName("freshness is the elapsed time between publish and durability")
  void recordsFreshnessAsElapsedTimeSincePublish() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ConsumerMetrics metrics = new ConsumerMetrics(registry);

    metrics.recordFreshness(1_000_000L, 1_000_250L);
    metrics.recordFreshness(1_000_000L, 1_001_500L);

    Timer freshness = registry.get("skier.pipeline.freshness").timer();
    assertThat(freshness.count()).isEqualTo(2);
    assertThat(freshness.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(1750.0);
    assertThat(freshness.max(TimeUnit.MILLISECONDS)).isEqualTo(1500.0);
  }

  @Test
  @DisplayName("an event carrying no publish timestamp is counted, not silently skipped")
  void countsUnstampedEventsRatherThanDroppingThem() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ConsumerMetrics metrics = new ConsumerMetrics(registry);

    metrics.recordFreshness(null, 1_000_000L);

    assertThat(registry.get("skier.pipeline.freshness.unstamped").counter().count()).isEqualTo(1.0);
    assertThat(registry.get("skier.pipeline.freshness").timer().count()).isZero();
  }

  @Test
  @DisplayName("a publish timestamp in the future is counted as skew, not recorded as freshness")
  void countsBackwardsClocksSeparatelyFromFreshness() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ConsumerMetrics metrics = new ConsumerMetrics(registry);

    metrics.recordFreshness(1_000_500L, 1_000_000L);

    assertThat(registry.get("skier.pipeline.freshness.skewed").counter().count()).isEqualTo(1.0);
    assertThat(registry.get("skier.pipeline.freshness").timer().count()).isZero();
  }
}
