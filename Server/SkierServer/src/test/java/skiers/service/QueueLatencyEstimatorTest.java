package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class QueueLatencyEstimatorTest {

  private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

  /** A 1000ms window, which TICK_NANOS divides into exactly five ticks. */
  private static QueueLatencyEstimator estimator() {
    return new QueueLatencyEstimator(Duration.ofMillis(1000));
  }

  @Test
  @DisplayName("the first sample cannot produce a rate, and says so rather than guessing zero")
  void firstSampleIsUnresolved() {
    QueueLatencyEstimator estimator = estimator();

    estimator.sample(150, 0, 0L);

    assertThat(estimator.latencySeconds()).isNaN();
    assertThat(estimator.drainRatePerSecond()).isNaN();
    assertThat(estimator.unresolvedSamples()).isEqualTo(1);
  }

  @Test
  @DisplayName("a window that has not yet filled stays unresolved")
  void partialWindowIsUnresolved() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick < 4; tick++) {
      estimator.sample(150, tick * 20, tick * TICK_NANOS);
    }

    assertThat(estimator.latencySeconds()).isNaN();
    assertThat(estimator.unresolvedSamples()).isEqualTo(4);
  }

  @Test
  @DisplayName("a steady queue reports drain rate equal to publish rate, and depth/rate as latency")
  void steadyStateLatencyIsDepthOverDrainRate() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(150, tick * 20, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isCloseTo(100.0, offset(0.001));
    assertThat(estimator.latencySeconds()).isCloseTo(1.5, offset(0.001));
    assertThat(estimator.unresolvedSamples()).isEqualTo(5);
  }

  @Test
  @DisplayName("a growing queue charges the growth against the drain rate")
  void growingQueueReducesTheDrainEstimate() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(200 + tick * 8, tick * 20, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isCloseTo(60.0, offset(0.001));
    assertThat(estimator.latencySeconds()).isCloseTo(280 / 60.0, offset(0.001));
  }

  @Test
  @DisplayName("a queue draining with no publishes reports the drain rate anyway")
  void drainWithNoPublishes() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(1000 - tick * 25, 500, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isCloseTo(125.0, offset(0.001));
    assertThat(estimator.latencySeconds()).isCloseTo(750 / 125.0, offset(0.001));
  }

  @Test
  @DisplayName("an empty queue is zero latency however stalled the consumer is")
  void emptyQueueIsZeroLatencyNotInfinity() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(0, 0, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isEqualTo(0.0);
    assertThat(estimator.latencySeconds()).isEqualTo(0.0);
  }

  @Test
  @DisplayName("a consumer that stops draining a non-empty queue reports unbounded latency")
  void stalledConsumerIsUnboundedNotZero() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(500, 0, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isEqualTo(0.0);
    assertThat(estimator.latencySeconds()).isInfinite().isPositive();
  }

  @Test
  @DisplayName("a drain rate that collapses mid-run is followed within one window")
  void collapsingDrainRateIsPickedUpWithinOneWindow() {
    QueueLatencyEstimator estimator = estimator();

    int tick = 0;
    for (; tick <= 10; tick++) {
      estimator.sample(150, tick * 20, tick * TICK_NANOS);
    }
    assertThat(estimator.latencySeconds()).isCloseTo(1.5, offset(0.001));

    int depth = 150;
    double published = tick * 20;
    for (int stalled = 0; stalled < 10; stalled++, tick++) {
      depth += 20;
      published += 20;
      estimator.sample(depth, published, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isEqualTo(0.0);
    assertThat(estimator.latencySeconds()).isInfinite().isPositive();
  }

  @Test
  @DisplayName("two samples at the same instant do not divide by zero")
  void repeatedTimestampIsUnresolved() {
    QueueLatencyEstimator estimator = estimator();

    estimator.sample(150, 0, 42L);
    estimator.sample(150, 20, 42L);

    assertThat(estimator.latencySeconds()).isNaN();
    assertThat(estimator.unresolvedSamples()).isEqualTo(2);
  }

  @Test
  @DisplayName("a sample older than the last one is refused rather than producing a negative rate")
  void backwardsTimeIsUnresolved() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(150, tick * 20, tick * TICK_NANOS);
    }
    assertThat(estimator.latencySeconds()).isCloseTo(1.5, offset(0.001));

    estimator.sample(150, 300, -TICK_NANOS);

    assertThat(estimator.latencySeconds()).isNaN();
  }

  @Test
  @DisplayName("another instance's publishes make this instance over-state queue latency")
  void arrivalsFromAnotherInstanceBiasTheEstimateConservatively() {
    QueueLatencyEstimator estimator = estimator();

    // Two servers publish 100/s each; this instance sees only its own half of the arrivals.
    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(150, tick * 20, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isCloseTo(100.0, offset(0.001));
    assertThat(estimator.latencySeconds())
        .isCloseTo(1.5, offset(0.001))
        .isGreaterThan(150.0 / 200.0);
  }

  @Test
  @DisplayName("depth growing without this instance publishing clamps the drain rate at zero")
  void foreignArrivalsClampTheDrainRateAtZero() {
    QueueLatencyEstimator estimator = estimator();

    for (int tick = 0; tick <= 10; tick++) {
      estimator.sample(150 + tick * 40, 0, tick * TICK_NANOS);
    }

    assertThat(estimator.drainRatePerSecond()).isEqualTo(0.0);
    assertThat(estimator.latencySeconds()).isInfinite().isPositive();
  }
}
