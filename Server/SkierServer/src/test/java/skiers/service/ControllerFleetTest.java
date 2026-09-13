package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.metrics.IngestMetrics;

class ControllerFleetTest {

  private static final double TICK_SECONDS = 0.2;

  private static final int DEFAULT_FLOOR = new SkierProperties().getAdmission().getMinRate();

  private static final class Fleet {

    private final AtomicInteger sharedDepth = new AtomicInteger();
    private final List<RateLimiter> limiters = new ArrayList<>();
    private final List<QueueMonitor> monitors = new ArrayList<>();

    Fleet(int instances, int initialRate) {
      this(instances, initialRate, FleetRegistry.singleInstance());
    }

    Fleet(int instances, int initialRate, FleetRegistry registry) {
      RabbitAdmin admin = mock(RabbitAdmin.class);
      when(admin.getQueueInfo(anyString()))
          .thenAnswer(
              invocation -> new QueueInformation(Constants.MAIN_QUEUE, sharedDepth.get(), 1));

      for (int i = 0; i < instances; i++) {
        SkierProperties properties = new SkierProperties();
        properties.getAdmission().setInitialCapacity(initialRate);
        // Refill pushed far into the future, so the bucket cannot race the simulated ticks.
        properties.getAdmission().setRefillIntervalMs(600_000);

        RateLimiter limiter = new RateLimiter(properties, new SimpleMeterRegistry(), registry);
        limiter.init();
        limiters.add(limiter);
        // One registry per instance: the depth gauge has one name, so a shared one keeps one.
        monitors.add(
            new QueueMonitor(
                admin,
                limiter,
                mock(IngestMetrics.class),
                properties,
                new SimpleMeterRegistry(),
                registry));
      }
    }

    void observeDepth(int depth) {
      sharedDepth.set(depth);
      monitors.forEach(QueueMonitor::adjustRateBasedOnQueueDepth);
    }

    long aggregateRate() {
      return limiters.stream().mapToLong(RateLimiter::getCurrentRate).sum();
    }

    int size() {
      return limiters.size();
    }
  }

  @Test
  @DisplayName("multiplicative decrease is scale-free: N instances halving halve the aggregate")
  void multiplicativeDecreaseIsScaleFree() {
    for (int instances : new int[] {1, 2, 3}) {
      Fleet fleet = new Fleet(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.MAX_QUEUE_SIZE);

      assertThat(fleet.aggregateRate())
          .as("aggregate rate after one overload tick with %d instance(s)", instances)
          .isEqualTo(before / 2);
    }
  }

  @Test
  @DisplayName("additive decrease costs the fleet N times the per-instance step")
  void additiveDecreaseScalesWithInstanceCount() {
    for (int instances : new int[] {1, 2, 3}) {
      Fleet fleet = new Fleet(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.TARGET_QUEUE_SIZE + 1);

      assertThat(before - fleet.aggregateRate())
          .as("aggregate permits/s surrendered by %d instance(s) in one tick", instances)
          .isEqualTo(1000L * instances);
    }
  }

  @Test
  @DisplayName("additive increase also scales with N, so the fleet recovers N times faster")
  void additiveIncreaseScalesWithInstanceCount() {
    for (int instances : new int[] {1, 2, 3}) {
      Fleet fleet = new Fleet(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.MIN_QUEUE_SIZE - 1);

      assertThat(fleet.aggregateRate() - before)
          .as("aggregate permits/s reclaimed by %d instance(s) in one tick", instances)
          .isEqualTo(10L * instances);
    }
  }

  @Test
  @DisplayName("the 100:1 recovery asymmetry is unchanged by instance count")
  void asymmetryRatioIsScaleFree() {
    for (int instances : new int[] {1, 2, 3}) {
      Fleet fleet = new Fleet(instances, 4000);

      fleet.observeDepth(Constants.TARGET_QUEUE_SIZE + 1);
      long surrendered = 4000L * instances - fleet.aggregateRate();
      long afterDecrease = fleet.aggregateRate();
      fleet.observeDepth(0);
      long reclaimed = fleet.aggregateRate() - afterDecrease;

      assertThat(surrendered / reclaimed)
          .as("decrease:increase ratio with %d instance(s)", instances)
          .isEqualTo(100L);
    }
  }

  @Test
  @DisplayName("the fleet cannot shed below N times the documented rate floor")
  void rateFloorScalesWithInstanceCount() {
    for (int instances : new int[] {1, 2, 3}) {
      Fleet fleet = new Fleet(instances, Constants.MAX_RATE);
      for (int tick = 0; tick < 100; tick++) {
        fleet.observeDepth(Constants.MAX_QUEUE_SIZE + 500);
      }

      assertThat(fleet.aggregateRate())
          .as("aggregate floor with %d instance(s)", instances)
          .isEqualTo((long) DEFAULT_FLOOR * instances);
    }
  }

  private static Fleet coordinated(int instances, int initialRate) {
    return new Fleet(instances, initialRate, () -> instances);
  }

  @Test
  @DisplayName("a coordinated fleet's floor is the configured floor, not N times it")
  void coordinatedFloorIsTheConfiguredFloor() {
    for (int instances : new int[] {1, 2, 3, 5}) {
      Fleet fleet = coordinated(instances, Constants.MAX_RATE);
      for (int tick = 0; tick < 100; tick++) {
        fleet.observeDepth(Constants.MAX_QUEUE_SIZE + 500);
      }

      assertThat(fleet.aggregateRate())
          .as("aggregate floor with %d coordinated instance(s)", instances)
          .isLessThanOrEqualTo(DEFAULT_FLOOR)
          .isGreaterThan((long) DEFAULT_FLOOR - instances);
    }
  }

  private static FleetRegistry unsized() {
    return new FleetRegistry() {
      @Override
      public int size() {
        return 1;
      }

      @Override
      public boolean isSized() {
        return false;
      }
    };
  }

  @Test
  @DisplayName("an instance that has never sized the fleet does not take the whole floor")
  void anUnsizedInstanceTakesTheMinimumFloor() {
    Fleet coordinated = coordinated(2, Constants.MAX_RATE);
    Fleet newcomer = new Fleet(1, Constants.MAX_RATE, unsized());
    for (int tick = 0; tick < 100; tick++) {
      coordinated.observeDepth(Constants.MAX_QUEUE_SIZE + 500);
      newcomer.observeDepth(Constants.MAX_QUEUE_SIZE + 500);
    }

    assertThat(newcomer.aggregateRate())
        .as("the newcomer's own floor, with no observed fleet size to divide by")
        .isEqualTo(1L);
    // share() floors at 1, so an unsized instance costs the aggregate one permit/s.
    assertThat(coordinated.aggregateRate() + newcomer.aggregateRate())
        .as("aggregate floor of a coordinated pair plus one instance that never reached Redis")
        .isLessThanOrEqualTo(DEFAULT_FLOOR + 1L);
  }

  @Test
  @DisplayName("a coordinated fleet surrenders one decrease step per tick, not N")
  void coordinatedAdditiveDecreaseIsOneStep() {
    for (int instances : new int[] {1, 2, 3, 5}) {
      Fleet fleet = coordinated(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.TARGET_QUEUE_SIZE + 1);

      assertThat(before - fleet.aggregateRate())
          .as(
              "aggregate permits/s surrendered by %d coordinated instance(s) in one tick",
              instances)
          .isLessThanOrEqualTo(1000L)
          .isGreaterThan(1000L - instances);
    }
  }

  @Test
  @DisplayName("a coordinated fleet reclaims one increase step per tick, not N")
  void coordinatedAdditiveIncreaseIsOneStep() {
    for (int instances : new int[] {1, 2, 5, 10}) {
      Fleet fleet = coordinated(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.MIN_QUEUE_SIZE - 1);

      assertThat(fleet.aggregateRate() - before)
          .as("aggregate permits/s reclaimed by %d coordinated instance(s) in one tick", instances)
          .isLessThanOrEqualTo(10L)
          .isGreaterThan(10L - instances);
    }
  }

  @Test
  @DisplayName("halving stays per-instance under coordination, because it is already correct")
  void coordinatedMultiplicativeDecreaseIsStillScaleFree() {
    for (int instances : new int[] {1, 2, 3, 5}) {
      Fleet fleet = coordinated(instances, 4000);
      long before = fleet.aggregateRate();

      fleet.observeDepth(Constants.MAX_QUEUE_SIZE);

      assertThat(fleet.aggregateRate())
          .as("aggregate rate after one overload tick with %d coordinated instance(s)", instances)
          .isEqualTo(before / 2);
    }
  }

  @Test
  @DisplayName("a fleet larger than the increase step can still recover")
  void aFleetLargerThanTheIncreaseStepCanStillRecover() {
    // 10 permits/s shared 50 ways truncates to 0, so share() floors the step at 1.
    Fleet fleet = coordinated(50, 4000);
    long before = fleet.aggregateRate();

    fleet.observeDepth(Constants.MIN_QUEUE_SIZE - 1);

    assertThat(fleet.aggregateRate() - before)
        .as("a 50-instance fleet must still reclaim something")
        .isPositive();
  }

  private record Run(int instances, long admitted, long shed, int finalDepth, int peakDepth) {

    double shedFraction() {
      return (double) shed / (admitted + shed);
    }
  }

  private Run simulate(int instances, int offeredPerSecond, int drainPerSecond, int ticks) {
    Fleet fleet = new Fleet(instances, 4000);
    double offeredPerInstancePerTick = offeredPerSecond * TICK_SECONDS / instances;
    double drainPerTick = drainPerSecond * TICK_SECONDS;

    double admitted = 0;
    double depth = 0;
    int peakDepth = 0;

    for (int tick = 0; tick < ticks; tick++) {
      fleet.observeDepth((int) depth);

      double admittedThisTick = 0;
      for (RateLimiter limiter : fleet.limiters) {
        double capacityThisTick = limiter.getCurrentRate() * TICK_SECONDS;
        admittedThisTick += Math.min(offeredPerInstancePerTick, capacityThisTick);
      }
      admitted += admittedThisTick;

      depth = Math.max(0, depth + admittedThisTick - drainPerTick);
      peakDepth = Math.max(peakDepth, (int) depth);
    }

    // Shed comes from the offered total: rounding each instance's share drifts the arms apart.
    long offeredTotal = Math.round(offeredPerSecond * TICK_SECONDS * ticks);
    long admittedTotal = Math.round(admitted);
    return new Run(
        fleet.size(), admittedTotal, offeredTotal - admittedTotal, (int) depth, peakDepth);
  }

  @Test
  @DisplayName("a fleet holds the queue no better than one instance under identical offered load")
  void closedLoopCorrectionMagnitude() {
    Run one = simulate(1, 4000, 2000, 600);
    Run two = simulate(2, 4000, 2000, 600);
    Run three = simulate(3, 4000, 2000, 600);

    System.out.printf("%n  instances  admitted      shed   shed%%   peak depth   final depth%n");
    for (Run run : new Run[] {one, two, three}) {
      System.out.printf(
          "  %9d  %8d  %8d  %5.1f%%  %11d  %12d%n",
          run.instances(),
          run.admitted(),
          run.shed(),
          run.shedFraction() * 100,
          run.peakDepth(),
          run.finalDepth());
    }

    assertThat(two.admitted() + two.shed()).isEqualTo(one.admitted() + one.shed());
    assertThat(three.admitted() + three.shed()).isEqualTo(one.admitted() + one.shed());

    assertThat(two.peakDepth())
        .as("two instances overshoot the backlog further than one")
        .isGreaterThan(one.peakDepth());
    assertThat(three.peakDepth())
        .as("three instances overshoot the backlog further than two")
        .isGreaterThan(two.peakDepth());

    assertThat(two.shedFraction())
        .as("a two-instance fleet sheds more of the same offered load than one instance")
        .isGreaterThan(one.shedFraction());
    assertThat(three.shedFraction())
        .as("a three-instance fleet sheds more of the same offered load than one instance")
        .isGreaterThan(one.shedFraction());
  }
}
