package skiers.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import skiers.Constants;
import skiers.config.SkierProperties;

/**
 * Token-bucket admission control with a runtime-adjustable rate. {@code capacity} is the rate in
 * permits/second and doubles as the bucket depth, so the largest burst absorbed is one second of
 * steady-state traffic. {@link QueueMonitor} moves the rate from observed queue depth.
 */
@Service
public class RateLimiter {

  private static final Logger logger = LoggerFactory.getLogger(RateLimiter.class);

  private final SkierProperties.Admission config;
  private final ScheduledExecutorService scheduler;

  private final int minRate;

  private final FleetRegistry fleet;

  private final AtomicInteger capacity = new AtomicInteger();
  private final AtomicInteger tokens = new AtomicInteger();

  /**
   * Fractional permits carried between refills, scaled by 1000. Integer truncation of the per-tick
   * rate leaks permits on every tick without it, and starves any rate below one permit per tick.
   */
  private final AtomicLong carryMillis = new AtomicLong();

  private final AtomicLong granted = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

  /** The bucket delivers the decided rate only if this loop runs on time; the gap is a maximum. */
  private final AtomicLong refillInvocations = new AtomicLong();

  private final AtomicLong refillMaxGapNanos = new AtomicLong();
  private final AtomicLong lastRefillNanos = new AtomicLong();

  /** Discarded permits, so {@code granted + discarded + tokens} accounts for the whole budget. */
  private final AtomicLong permitsDiscarded = new AtomicLong();

  public RateLimiter(SkierProperties properties, MeterRegistry registry, FleetRegistry fleet) {
    this.config = properties.getAdmission();
    this.minRate = config.getMinRate();
    this.fleet = fleet;
    // Refused rather than clamped. A floor nobody configured would surface only as admission
    // behaving unlike its own configuration, and a floor of 0 admits nothing.
    if (minRate < 1 || minRate > Constants.MAX_RATE) {
      throw new IllegalArgumentException(
          "skier.admission.min-rate must be in [1, "
              + Constants.MAX_RATE
              + "] permits/s but was "
              + minRate);
    }
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "admission-refill");
              thread.setDaemon(true);
              return thread;
            });

    Gauge.builder("skier.admission.rate", capacity, AtomicInteger::get)
        .description("Current admission rate in permits/second")
        .register(registry);
    Gauge.builder("skier.admission.rate.floor", this, RateLimiter::effectiveFloor)
        .description("This instance's share of the admission floor in permits/second")
        .register(registry);
    Gauge.builder("skier.fleet.size", fleet, FleetRegistry::size)
        .description("Server instances believed to be steering admission against the same queue")
        .register(registry);
    Gauge.builder("skier.admission.tokens", tokens, AtomicInteger::get)
        .description("Permits currently available")
        .register(registry);
    Gauge.builder("skier.admission.granted", granted, AtomicLong::get)
        .description("Permits granted since start")
        .register(registry);
    Gauge.builder("skier.admission.rejected", rejected, AtomicLong::get)
        .description("Permit requests denied since start")
        .register(registry);
    Gauge.builder("skier.admission.refill.invocations", refillInvocations, AtomicLong::get)
        .description("Refill loop invocations since start; nominally 1000/refill-interval-ms per s")
        .register(registry);
    Gauge.builder("skier.admission.refill.gap.max", this, RateLimiter::refillMaxGapSeconds)
        .baseUnit("seconds")
        .description("Worst observed interval between two consecutive refill loop invocations")
        .register(registry);
    Gauge.builder("skier.admission.permits.discarded", permitsDiscarded, AtomicLong::get)
        .description("Permits the refill budget produced that the bucket could not hold")
        .register(registry);
  }

  @PostConstruct
  public void init() {
    int initial = clamp(config.getInitialCapacity());
    capacity.set(initial);
    tokens.set(initial);
    scheduler.scheduleAtFixedRate(
        this::refill,
        config.getRefillIntervalMs(),
        config.getRefillIntervalMs(),
        TimeUnit.MILLISECONDS);
    logger.info(
        "Admission control armed at {} permits/s (refill every {}ms, max wait {}ms)",
        initial,
        config.getRefillIntervalMs(),
        config.getMaxWaitMs());
  }

  @PreDestroy
  public void stop() {
    scheduler.shutdownNow();
  }

  /** Takes one permit if available. Never blocks. */
  public boolean tryAcquire() {
    boolean acquired = tokens.getAndUpdate(current -> current > 0 ? current - 1 : current) > 0;
    if (acquired) {
      granted.incrementAndGet();
    } else {
      rejected.incrementAndGet();
    }
    return acquired;
  }

  public boolean tryAcquire(long maxWaitMs) throws InterruptedException {
    if (tryAcquire()) {
      return true;
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
    while (System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(config.getWaitSliceMs());
      if (tryAcquire()) {
        return true;
      }
    }
    return false;
  }

  public void adjustRateUp(int newRate) {
    setRate(Math.min(newRate, Constants.MAX_RATE));
  }

  public void adjustRateDown(int newRate) {
    setRate(Math.max(newRate, effectiveFloor()));
  }

  private void setRate(int newRate) {
    int adjusted = clamp(newRate);
    capacity.set(adjusted);
    // Shrinking the bucket must shrink what is in it, or the previous rate's backlog goes first.
    tokens.updateAndGet(current -> Math.min(current, adjusted));
  }

  /**
   * Mints one tick's worth of permits, per invocation rather than per elapsed second. A missed tick
   * is repaid as a catch-up burst into a bucket one second deep, and the excess is discarded.
   */
  void refill() {
    long now = System.nanoTime();
    long previous = lastRefillNanos.getAndSet(now);
    if (previous != 0L) {
      long gap = now - previous;
      refillMaxGapNanos.accumulateAndGet(gap, Math::max);
    }
    refillInvocations.incrementAndGet();

    int currentCapacity = capacity.get();
    long budget = carryMillis.get() + (long) currentCapacity * config.getRefillIntervalMs();
    long whole = budget / 1000L;
    carryMillis.set(budget - whole * 1000L);
    if (whole <= 0) {
      return;
    }
    int added = addTokens((int) Math.min(whole, currentCapacity), currentCapacity);
    if (added < whole) {
      permitsDiscarded.addAndGet(whole - added);
    }
  }

  private int addTokens(int toAdd, int cap) {
    while (true) {
      int current = tokens.get();
      int next = Math.min(current + toAdd, cap);
      if (tokens.compareAndSet(current, next)) {
        return Math.max(0, next - current);
      }
    }
  }

  private int clamp(int rate) {
    return Math.max(effectiveFloor(), Math.min(rate, Constants.MAX_RATE));
  }

  public int getCurrentRate() {
    return capacity.get();
  }

  /**
   * This instance's share of the configured floor, so N replicas do not each admit the whole floor.
   * Read on every adjustment, because the fleet size changes while the process runs. An unsized
   * instance takes the share of an arbitrarily large fleet, one permit/s, rather than of a guess.
   */
  public int effectiveFloor() {
    return FleetRegistry.share(minRate, fleet.isSized() ? fleet.size() : Integer.MAX_VALUE);
  }

  /** The fleet-wide floor as configured, before it is shared out. */
  public int configuredFloor() {
    return minRate;
  }

  public int availableTokens() {
    return tokens.get();
  }

  public long grantedCount() {
    return granted.get();
  }

  public long rejectedCount() {
    return rejected.get();
  }

  public long refillInvocations() {
    return refillInvocations.get();
  }

  public double refillMaxGapSeconds() {
    return refillMaxGapNanos.get() / 1_000_000_000.0;
  }

  public long permitsDiscardedCount() {
    return permitsDiscarded.get();
  }
}
