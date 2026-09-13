package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.metrics.IngestMetrics;

class QueueMonitorLatencySetpointTest {

  private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

  private static final int DEFAULT_FLOOR = new SkierProperties().getAdmission().getMinRate();

  private enum Action {
    SHED,
    HOLD,
    RAISE
  }

  private static final class Harness {

    private final RateLimiter limiter;
    private final QueueMonitor monitor;
    private final IngestMetrics metrics = mock(IngestMetrics.class);
    private int depth;
    private double published;
    private long nanos;

    Harness(SkierProperties.SetpointMode mode, Duration targetLatency) {
      SkierProperties properties = new SkierProperties();
      properties.getAdmission().setInitialCapacity(4000);
      properties.getAdmission().setRefillIntervalMs(600_000);
      properties.getQueueMonitor().setSetpointMode(mode);
      if (targetLatency != null) {
        properties.getQueueMonitor().setTargetLatency(targetLatency);
        properties.getQueueMonitor().setMaxLatency(targetLatency.multipliedBy(4).dividedBy(3));
        properties.getQueueMonitor().setMinLatency(targetLatency.multipliedBy(2).dividedBy(3));
      }
      properties.getQueueMonitor().setDrainRateWindow(Duration.ofSeconds(1));

      RabbitAdmin admin = mock(RabbitAdmin.class);
      when(admin.getQueueInfo(anyString()))
          .thenAnswer(invocation -> new QueueInformation(Constants.MAIN_QUEUE, depth, 1));
      when(metrics.acceptedCount()).thenAnswer(invocation -> published);

      limiter =
          new RateLimiter(properties, new SimpleMeterRegistry(), FleetRegistry.singleInstance());
      limiter.init();
      monitor =
          new QueueMonitor(
              admin,
              limiter,
              metrics,
              properties,
              new SimpleMeterRegistry(),
              FleetRegistry.singleInstance(),
              () -> nanos);
    }

    Action classify(int steadyDepth, int drainPerSecond) {
      this.depth = steadyDepth;
      double perTick = drainPerSecond * (TICK_NANOS / 1_000_000_000.0);
      // Long enough for the drain-rate window to fill several times over.
      for (int tick = 0; tick < 20; tick++) {
        nanos += TICK_NANOS;
        published += perTick;
        monitor.adjustRateBasedOnQueueDepth();
      }
      // Reset first: a rate already parked at the floor cannot shed, and reads as a hold.
      limiter.adjustRateUp(4000);
      int before = limiter.getCurrentRate();
      nanos += TICK_NANOS;
      published += perTick;
      monitor.adjustRateBasedOnQueueDepth();
      int after = limiter.getCurrentRate();
      if (after < before) {
        return Action.SHED;
      }
      return after > before ? Action.RAISE : Action.HOLD;
    }

    double observedLatency() {
      return monitor.observedQueueLatencySeconds();
    }
  }

  private static Harness depthMode() {
    return new Harness(SkierProperties.SetpointMode.DEPTH, null);
  }

  private static Harness latencyMode(Duration target) {
    return new Harness(SkierProperties.SetpointMode.LATENCY, target);
  }

  @Test
  @DisplayName("a depth setpoint protects a different queue latency at each drain rate")
  void depthSetpointProtectsADifferentLatencyAtEachDrainRate() {
    assertThat(depthMode().classify(150, 100)).isEqualTo(Action.HOLD);
    assertThat(depthMode().classify(1500, 1000)).isEqualTo(Action.SHED);

    Harness fast = depthMode();
    assertThat(fast.classify(150, 1000)).isEqualTo(Action.HOLD);
    assertThat(fast.observedLatency()).isCloseTo(0.15, org.assertj.core.data.Offset.offset(0.01));
  }

  @Test
  @DisplayName("a latency setpoint treats the same queueing delay identically at any drain rate")
  void latencySetpointIsInvariantToDrainRate() {
    Duration target = Duration.ofMillis(1500);

    assertThat(latencyMode(target).classify(150, 100)).isEqualTo(Action.HOLD);
    assertThat(latencyMode(target).classify(1500, 1000)).isEqualTo(Action.HOLD);

    assertThat(latencyMode(target).classify(240, 100)).isEqualTo(Action.SHED);
    assertThat(latencyMode(target).classify(2400, 1000)).isEqualTo(Action.SHED);

    assertThat(latencyMode(target).classify(20, 10)).isEqualTo(Action.SHED);
  }

  // Each depth below sits in a different zone of the depth thresholds than of the latency ones.

  @Test
  @DisplayName("latency at or above the ceiling halves the rate")
  void multiplicativeDecreaseAtLatencyCeiling() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    assertThat(harness.classify(120, 50)).isEqualTo(Action.SHED);
    assertThat(harness.limiter.getCurrentRate()).isEqualTo(2000);
  }

  @Test
  @DisplayName("latency above the target but under the ceiling sheds a fixed step")
  void additiveDecreaseAboveLatencyTarget() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    assertThat(harness.classify(80, 50)).isEqualTo(Action.SHED);
    assertThat(harness.limiter.getCurrentRate()).isEqualTo(3000);
  }

  @Test
  @DisplayName("latency below the floor reclaims a small step")
  void additiveIncreaseBelowLatencyFloor() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    assertThat(harness.classify(900, 1000)).isEqualTo(Action.RAISE);
    assertThat(harness.limiter.getCurrentRate()).isEqualTo(4010);
  }

  @Test
  @DisplayName("latency inside the deadband leaves the rate alone")
  void latencyDeadbandHoldsSteady() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    assertThat(harness.classify(1200, 1000)).isEqualTo(Action.HOLD);
    assertThat(harness.limiter.getCurrentRate()).isEqualTo(4000);
  }

  @Test
  @DisplayName("an empty queue reclaims rate rather than reading as an unknown latency")
  void emptyQueueRaisesTheRate() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    assertThat(harness.classify(0, 0)).isEqualTo(Action.RAISE);
  }

  @Test
  @DisplayName("latency mode holds the rate until the drain-rate window has filled")
  void unresolvedLatencyHoldsTheRate() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    harness.depth = 5000;

    // Three ticks span 400ms, well short of the 1s drain-rate window.
    for (int tick = 0; tick < 3; tick++) {
      harness.nanos += TICK_NANOS;
      harness.monitor.adjustRateBasedOnQueueDepth();
    }

    assertThat(harness.limiter.getCurrentRate()).isEqualTo(4000);
    assertThat(harness.monitor.observedQueueLatencySeconds()).isNaN();
    assertThat(harness.monitor.lastObservedDepth()).isEqualTo(5000);
  }

  @Test
  @DisplayName("a stalled consumer with a backlog halves the rate rather than holding it")
  void stalledConsumerShedsHard() {
    Harness harness = latencyMode(Duration.ofMillis(1500));
    harness.depth = 800;

    for (int tick = 0; tick < 20; tick++) {
      harness.nanos += TICK_NANOS;
      harness.monitor.adjustRateBasedOnQueueDepth();
    }

    assertThat(harness.monitor.observedQueueLatencySeconds()).isInfinite();
    assertThat(harness.limiter.getCurrentRate()).isEqualTo(DEFAULT_FLOOR);
  }

  @Test
  @DisplayName("an unreachable broker holds the rate in latency mode too")
  void sampleFailureHoldsRate() {
    SkierProperties properties = new SkierProperties();
    properties.getAdmission().setInitialCapacity(4000);
    properties.getAdmission().setRefillIntervalMs(600_000);
    properties.getQueueMonitor().setSetpointMode(SkierProperties.SetpointMode.LATENCY);

    RabbitAdmin admin = mock(RabbitAdmin.class);
    when(admin.getQueueInfo(anyString()))
        .thenThrow(new org.springframework.amqp.AmqpConnectException(new RuntimeException("down")));
    RateLimiter limiter =
        new RateLimiter(properties, new SimpleMeterRegistry(), FleetRegistry.singleInstance());
    limiter.init();
    QueueMonitor monitor =
        new QueueMonitor(
            admin,
            limiter,
            mock(IngestMetrics.class),
            properties,
            new SimpleMeterRegistry(),
            FleetRegistry.singleInstance(),
            () -> 0L);

    monitor.adjustRateBasedOnQueueDepth();

    assertThat(limiter.getCurrentRate()).isEqualTo(4000);
    assertThat(monitor.observedQueueLatencySeconds()).isNaN();
  }

  @Test
  @DisplayName("the default latency thresholds preserve the depth thresholds' deadband geometry")
  void defaultLatencyThresholdsPreserveTheDeadbandRatios() {
    SkierProperties.QueueMonitor defaults = new SkierProperties().getQueueMonitor();

    double target = defaults.getTargetLatency().toMillis();
    double max = defaults.getMaxLatency().toMillis();
    double min = defaults.getMinLatency().toMillis();

    assertThat(max / target).isCloseTo(200.0 / 150.0, org.assertj.core.data.Offset.offset(0.001));
    assertThat(min / target).isCloseTo(100.0 / 150.0, org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  @DisplayName("the default mode is depth, so the tuned behaviour is what ships")
  void depthRemainsTheDefaultMode() {
    assertThat(new SkierProperties().getQueueMonitor().getSetpointMode())
        .isEqualTo(SkierProperties.SetpointMode.DEPTH);
  }
}
