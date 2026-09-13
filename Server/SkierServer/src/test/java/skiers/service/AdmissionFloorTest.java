package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.metrics.IngestMetrics;

class AdmissionFloorTest {

  /** Tick period, matching the {@code skier.queue-monitor.interval-ms} default. */
  private static final double TICK_SECONDS = 0.2;

  private static final int MEASURED_DRAIN_RATE = 80;

  private static final int TICKS = 1500;

  private record Run(
      int floor,
      int minRateObserved,
      int finalRate,
      int finalDepth,
      int peakDepth,
      long pinnedTicks,
      long pinnedTicksInFinalFifth) {}

  @Test
  @DisplayName("a floor above the drain rate is reported, and keeps being reported")
  void floorAboveTheDrainRateIsReportedForAsLongAsItLasts() {
    Run run = simulate(100, 4000, MEASURED_DRAIN_RATE, TICKS);

    assertThat(run.finalRate()).as("rate at the end of a sustained overload").isEqualTo(100);
    assertThat(run.finalDepth())
        .as("a floor above the drain rate cannot stabilise the queue")
        .isEqualTo(run.peakDepth())
        .isGreaterThan(5000);

    assertThat(run.pinnedTicks())
        .as(
            "ticks on which the system reported that its floor was the binding constraint "
                + "(-1 means the meter skier.admission.floor.pinned does not exist at all, which is "
                + "the state this test was written against)")
        .isPositive();
    assertThat(run.pinnedTicksInFinalFifth())
        .as("still reporting at the end of the run, not only during the initial retreat")
        .isPositive();
  }

  @Test
  @DisplayName("a floor below the drain rate lets the controller stabilise, and it stops reporting")
  void floorBelowTheDrainRateStabilisesAndStopsReporting() {
    Run run = simulate(25, 4000, MEASURED_DRAIN_RATE, TICKS);

    assertThat(run.minRateObserved())
        .as("lowest rate reached with a configured floor of 25 permits/s")
        .isEqualTo(25);

    assertThat(run.finalDepth())
        .as("a floor below the drain rate lets the controller clear the backlog")
        .isLessThan(Constants.MAX_QUEUE_SIZE);

    assertThat(run.pinnedTicks()).as("the initial retreat does pin the floor").isPositive();
    assertThat(run.pinnedTicksInFinalFifth())
        .as("but a floor that is not the binding constraint stops reporting once recovered")
        .isZero();
  }

  @Test
  @DisplayName("a floor outside the rate range is refused at startup rather than clamped silently")
  void anOutOfRangeFloorFailsFast() {
    assertThatThrownBy(
            () ->
                new RateLimiter(
                    propertiesWithFloor(0),
                    new SimpleMeterRegistry(),
                    FleetRegistry.singleInstance()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("skier.admission.min-rate");

    assertThatThrownBy(
            () ->
                new RateLimiter(
                    propertiesWithFloor(Constants.MAX_RATE + 1),
                    new SimpleMeterRegistry(),
                    FleetRegistry.singleInstance()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("skier.admission.min-rate");
  }

  private Run simulate(int floor, int offeredPerSecond, int drainPerSecond, int ticks) {
    SkierProperties properties = propertiesWithFloor(floor);
    MeterRegistry registry = new SimpleMeterRegistry();

    RateLimiter limiter = new RateLimiter(properties, registry, FleetRegistry.singleInstance());
    limiter.init();

    AtomicInteger sharedDepth = new AtomicInteger();
    RabbitAdmin admin = mock(RabbitAdmin.class);
    when(admin.getQueueInfo(anyString()))
        .thenAnswer(invocation -> new QueueInformation(Constants.MAIN_QUEUE, sharedDepth.get(), 1));
    QueueMonitor monitor =
        new QueueMonitor(
            admin,
            limiter,
            mock(IngestMetrics.class),
            properties,
            registry,
            FleetRegistry.singleInstance());

    double offeredPerTick = offeredPerSecond * TICK_SECONDS;
    double drainPerTick = drainPerSecond * TICK_SECONDS;

    double depth = 0;
    int peakDepth = 0;
    int minRateObserved = Integer.MAX_VALUE;
    long pinnedAtFourFifths = 0;
    int fourFifths = (ticks * 4) / 5;

    for (int tick = 0; tick < ticks; tick++) {
      sharedDepth.set((int) depth);
      monitor.adjustRateBasedOnQueueDepth();

      int rate = limiter.getCurrentRate();
      minRateObserved = Math.min(minRateObserved, rate);
      double admitted = Math.min(offeredPerTick, rate * TICK_SECONDS);
      depth = Math.max(0, depth + admitted - drainPerTick);
      peakDepth = Math.max(peakDepth, (int) depth);

      if (tick == fourFifths - 1) {
        pinnedAtFourFifths = pinnedTicks(registry);
      }
    }

    long pinned = pinnedTicks(registry);
    return new Run(
        floor,
        minRateObserved,
        limiter.getCurrentRate(),
        (int) depth,
        peakDepth,
        pinned,
        pinned - pinnedAtFourFifths);
  }

  private static SkierProperties propertiesWithFloor(int floor) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("skier.admission.min-rate", floor);
    values.put("skier.admission.initial-capacity", 4000);
    // Refill pushed far into the future, so the bucket cannot race the simulated ticks.
    values.put("skier.admission.refill-interval-ms", 600_000);

    return new Binder(new MapConfigurationPropertySource(values))
        .bind("skier", SkierProperties.class)
        .orElseThrow(() -> new IllegalStateException("skier.* did not bind at all"));
  }

  private static long pinnedTicks(MeterRegistry registry) {
    Counter counter = registry.find("skier.admission.floor.pinned").counter();
    return counter == null ? -1 : (long) counter.count();
  }
}
