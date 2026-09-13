package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import skiers.Constants;
import skiers.config.SkierProperties;

class RateLimiterTest {

  private static final int DEFAULT_FLOOR = new SkierProperties().getAdmission().getMinRate();

  private RateLimiter limiter;

  private RateLimiter build(int capacity, long refillMs, long maxWaitMs) {
    SkierProperties properties = new SkierProperties();
    properties.getAdmission().setInitialCapacity(capacity);
    properties.getAdmission().setRefillIntervalMs(refillMs);
    properties.getAdmission().setMaxWaitMs(maxWaitMs);
    properties.getAdmission().setWaitSliceMs(1);
    limiter =
        new RateLimiter(properties, new SimpleMeterRegistry(), FleetRegistry.singleInstance());
    limiter.init();
    return limiter;
  }

  private RateLimiter buildUnarmed(int rate, long refillMs) {
    SkierProperties properties = new SkierProperties();
    properties.getAdmission().setRefillIntervalMs(refillMs);
    properties.getAdmission().setMaxWaitMs(0);
    properties.getAdmission().setWaitSliceMs(1);
    limiter =
        new RateLimiter(properties, new SimpleMeterRegistry(), FleetRegistry.singleInstance());
    limiter.adjustRateUp(rate);
    return limiter;
  }

  @AfterEach
  void tearDown() {
    if (limiter != null) {
      limiter.stop();
    }
  }

  @Test
  @DisplayName("bucket starts full and drains exactly once per permit")
  void drainsExactlyOncePerPermit() {
    int capacity = DEFAULT_FLOOR;
    RateLimiter rateLimiter = build(capacity, 600_000, 0);

    for (int i = 0; i < capacity; i++) {
      assertThat(rateLimiter.tryAcquire()).as("permit %d", i).isTrue();
    }
    assertThat(rateLimiter.tryAcquire()).isFalse();
    assertThat(rateLimiter.grantedCount()).isEqualTo(capacity);
    assertThat(rateLimiter.rejectedCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("an out-of-range configured capacity is clamped rather than honoured")
  void configuredCapacityIsClamped() {
    assertThat(build(1, 600_000, 0).getCurrentRate()).isEqualTo(DEFAULT_FLOOR);
    limiter.stop();
    assertThat(build(Integer.MAX_VALUE, 600_000, 0).getCurrentRate()).isEqualTo(Constants.MAX_RATE);
  }

  @Test
  @DisplayName("never hands the same permit to two threads")
  void neverDoubleGrantsUnderContention() throws Exception {
    int capacity = 2000;
    RateLimiter rateLimiter = build(capacity, 600_000, 0);

    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    AtomicInteger granted = new AtomicInteger();

    List<Callable<Void>> tasks = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      tasks.add(
          () -> {
            for (int i = 0; i < 1000; i++) {
              if (rateLimiter.tryAcquire()) {
                granted.incrementAndGet();
              }
            }
            return null;
          });
    }
    for (Future<Void> future : pool.invokeAll(tasks)) {
      future.get();
    }
    pool.shutdownNow();

    assertThat(granted.get())
        .as("16 threads racing for %d permits must observe exactly %d grants", capacity, capacity)
        .isEqualTo(capacity);
  }

  @Test
  @DisplayName("refill delivers the configured rate even when it is not a multiple of the tick")
  void refillCarriesTheFractionalRemainder() throws Exception {
    int capacity = 150;
    RateLimiter rateLimiter = build(capacity, 10, 0);

    while (rateLimiter.tryAcquire()) {
      // drain
    }

    int acquired = 0;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (System.nanoTime() < deadline) {
      if (rateLimiter.tryAcquire()) {
        acquired++;
      } else {
        TimeUnit.MILLISECONDS.sleep(1);
      }
    }

    // Scheduler jitter makes this approximate; the band still separates 150/s from 100/s.
    assertThat(acquired).as("one second of refills at a configured 150/s").isBetween(120, 190);
  }

  @Test
  @DisplayName("rates below the tick granularity still admit traffic")
  void lowRatesAreNotStarved() throws Exception {
    RateLimiter rateLimiter = build(DEFAULT_FLOOR, 10, 0);
    while (rateLimiter.tryAcquire()) {
      // drain
    }

    TimeUnit.MILLISECONDS.sleep(250);
    assertThat(rateLimiter.tryAcquire()).isTrue();
  }

  @Test
  @DisplayName("bounded wait returns false instead of blocking indefinitely")
  void boundedWaitSheds() throws Exception {
    RateLimiter rateLimiter = build(DEFAULT_FLOOR, 600_000, 40);
    while (rateLimiter.tryAcquire()) {
      // drain
    }

    long startedAt = System.nanoTime();
    boolean admitted = rateLimiter.tryAcquire(40);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

    assertThat(admitted).isFalse();
    assertThat(elapsedMs)
        .as("must give up promptly rather than looping 100,000 times at 5ms")
        .isLessThan(1_000);
  }
}
