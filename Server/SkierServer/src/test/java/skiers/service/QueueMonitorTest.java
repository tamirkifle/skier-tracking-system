package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.metrics.IngestMetrics;

class QueueMonitorTest {

  private static final int DEFAULT_FLOOR = new SkierProperties().getAdmission().getMinRate();

  private RabbitAdmin rabbitAdmin;
  private RateLimiter rateLimiter;
  private QueueMonitor monitor;

  @BeforeEach
  void setUp() {
    rabbitAdmin = mock(RabbitAdmin.class);

    SkierProperties properties = new SkierProperties();
    properties.getAdmission().setInitialCapacity(4000);
    // Refill pushed far into the future, so no test tops the bucket up mid-run.
    properties.getAdmission().setRefillIntervalMs(600_000);

    rateLimiter =
        new RateLimiter(properties, new SimpleMeterRegistry(), FleetRegistry.singleInstance());
    rateLimiter.init();

    monitor =
        new QueueMonitor(
            rabbitAdmin,
            rateLimiter,
            mock(IngestMetrics.class),
            properties,
            new SimpleMeterRegistry(),
            FleetRegistry.singleInstance());
  }

  private void observeDepth(int depth) {
    when(rabbitAdmin.getQueueInfo(anyString()))
        .thenReturn(new QueueInformation(Constants.MAIN_QUEUE, depth, 1));
    monitor.adjustRateBasedOnQueueDepth();
  }

  @Test
  @DisplayName("depth at the hard ceiling halves the rate")
  void multiplicativeDecreaseAtCeiling() {
    observeDepth(Constants.MAX_QUEUE_SIZE);
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(2000);
  }

  @Test
  @DisplayName("depth above target sheds a fixed step")
  void additiveDecreaseAboveTarget() {
    observeDepth(Constants.TARGET_QUEUE_SIZE + 1);
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(3000);
  }

  @Test
  @DisplayName("depth below the floor reclaims a small step")
  void additiveIncreaseBelowFloor() {
    observeDepth(Constants.MIN_QUEUE_SIZE - 1);
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(4010);
  }

  @Test
  @DisplayName("depth inside the deadband leaves the rate alone")
  void deadbandHoldsSteady() {
    observeDepth(Constants.TARGET_QUEUE_SIZE);
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(4000);
  }

  @Test
  @DisplayName("decrease never falls below the configured floor")
  void decreaseRespectsFloor() {
    for (int i = 0; i < 50; i++) {
      observeDepth(Constants.MAX_QUEUE_SIZE + 500);
    }
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(DEFAULT_FLOOR);
  }

  @Test
  @DisplayName("increase never exceeds the configured ceiling")
  void increaseRespectsCeiling() {
    rateLimiter.adjustRateUp(Constants.MAX_RATE);
    observeDepth(0);
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(Constants.MAX_RATE);
  }

  @Test
  @DisplayName("an unreachable broker holds the rate instead of reading as an empty queue")
  void sampleFailureHoldsRate() {
    when(rabbitAdmin.getQueueInfo(anyString()))
        .thenThrow(new org.springframework.amqp.AmqpConnectException(new RuntimeException("down")));

    monitor.adjustRateBasedOnQueueDepth();

    assertThat(rateLimiter.getCurrentRate()).isEqualTo(4000);
    assertThat(monitor.lastObservedDepth()).isEqualTo(-1);
  }

  @Test
  @DisplayName("a null queue info holds the rate")
  void missingQueueHoldsRate() {
    when(rabbitAdmin.getQueueInfo(anyString())).thenReturn(null);

    monitor.adjustRateBasedOnQueueDepth();

    assertThat(rateLimiter.getCurrentRate()).isEqualTo(4000);
  }

  @Test
  @DisplayName("recovery from overload is far slower than the retreat into it")
  void recoveryIsDeliberatelyAsymmetric() {
    observeDepth(Constants.MAX_QUEUE_SIZE);
    int afterOneOverloadTick = rateLimiter.getCurrentRate();
    assertThat(afterOneOverloadTick).isEqualTo(2000);

    for (int i = 0; i < 10; i++) {
      observeDepth(0);
    }

    // 10 recovery ticks reclaim 100 permits/s of the 2000 surrendered by a single overload tick.
    assertThat(rateLimiter.getCurrentRate()).isEqualTo(2100);
  }
}
