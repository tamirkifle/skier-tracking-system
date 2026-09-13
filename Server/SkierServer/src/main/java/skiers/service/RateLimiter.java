package skiers.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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

  private final AtomicLong granted = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

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
}
