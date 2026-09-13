package skiers.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.metrics.IngestMetrics;

/**
 * Closed-loop controller: samples queue depth and steers the admission rate toward a setpoint.
 * AIMD, the same shape as TCP congestion control, so retreat costs one tick and recovery a hundred.
 *
 * <p>Every instance runs this loop against the shared queue, so the two additive steps and the
 * admission floor cost N times and are divided by the observed fleet size. Halving is scale-free,
 * since the sum of N halved rates is half the sum, so the multiplicative branch stays per instance.
 */
@Service
@ConditionalOnProperty(value = "skier.queue-monitor.enabled", matchIfMissing = true)
public class QueueMonitor {

  private static final Logger logger = LoggerFactory.getLogger(QueueMonitor.class);

  /** Permits/second the fleet surrenders per tick above the target, before the fleet divide. */
  private static final int DECREASE_STEP = 1000;

  /** Permits/second the fleet reclaims per tick below the minimum, before the fleet divide. */
  private static final int INCREASE_STEP = 10;

  private final RabbitAdmin rabbitAdmin;
  private final RateLimiter rateLimiter;
  private final IngestMetrics ingestMetrics;
  private final SkierProperties.QueueMonitor config;
  private final FleetRegistry fleet;
  private final QueueLatencyEstimator latencyEstimator;
  private final LongSupplier nanoTime;
  private final Counter floorPinned;
  private final AtomicInteger lastObservedDepth = new AtomicInteger(-1);
  private final AtomicInteger consecutiveSampleFailures = new AtomicInteger();

  /** Spring infers an injection point only from a single constructor, and there are two here. */
  @Autowired
  public QueueMonitor(
      RabbitAdmin rabbitAdmin,
      RateLimiter rateLimiter,
      IngestMetrics ingestMetrics,
      SkierProperties properties,
      MeterRegistry registry,
      FleetRegistry fleet) {
    this(rabbitAdmin, rateLimiter, ingestMetrics, properties, registry, fleet, System::nanoTime);
  }

  QueueMonitor(
      RabbitAdmin rabbitAdmin,
      RateLimiter rateLimiter,
      IngestMetrics ingestMetrics,
      SkierProperties properties,
      MeterRegistry registry,
      FleetRegistry fleet,
      LongSupplier nanoTime) {
    this.rabbitAdmin = rabbitAdmin;
    this.rateLimiter = rateLimiter;
    this.ingestMetrics = ingestMetrics;
    this.config = properties.getQueueMonitor();
    this.fleet = fleet;
    this.nanoTime = nanoTime;
    this.latencyEstimator = new QueueLatencyEstimator(config.getDrainRateWindow());

    Gauge.builder("skier.queue.depth", lastObservedDepth, AtomicInteger::get)
        .description("Messages ready in the lift-ride queue (-1 when unknown)")
        .register(registry);
    // Micrometer holds a gauge source weakly, so it lives in a field rather than a bare lambda.
    Gauge.builder("skier.queue.latency", latencyEstimator, QueueLatencyEstimator::latencySeconds)
        .description("Estimated seconds an event joining the queue now waits to be consumed")
        .baseUnit("seconds")
        .register(registry);
    Gauge.builder(
            "skier.queue.drain.rate", latencyEstimator, QueueLatencyEstimator::drainRatePerSecond)
        .description("Estimated events/second the consumer is removing from the queue")
        .register(registry);
    FunctionCounter.builder(
            "skier.queue.drain.unresolved",
            latencyEstimator,
            QueueLatencyEstimator::unresolvedSamples)
        .description("Controller ticks at which no drain rate could be computed")
        .register(registry);
    this.floorPinned =
        Counter.builder("skier.admission.floor.pinned")
            .description(
                "Controller ticks at which the rate was already at its floor and the backlog still"
                    + " called for a decrease")
            .register(registry);

    if (config.getSetpointMode() == SkierProperties.SetpointMode.LATENCY) {
      logger.info(
          "Queue-latency controller sampling every {}ms, target {}ms (max {}ms, min {}ms) "
              + "over a {}ms drain-rate window",
          config.getIntervalMs(),
          config.getTargetLatency().toMillis(),
          config.getMaxLatency().toMillis(),
          config.getMinLatency().toMillis(),
          config.getDrainRateWindow().toMillis());
    } else {
      logger.info(
          "Queue-depth controller sampling every {}ms, target depth {}",
          config.getIntervalMs(),
          Constants.TARGET_QUEUE_SIZE);
    }
  }

  /** What one tick decides to do with the admission rate. */
  private enum Zone {
    SEVERE,
    ABOVE,
    HOLD,
    BELOW
  }

  @Scheduled(
      fixedRateString = "${skier.queue-monitor.interval-ms:200}",
      initialDelayString = "${skier.queue-monitor.interval-ms:200}")
  public void adjustRateBasedOnQueueDepth() {
    Integer depth = sampleQueueDepth();
    if (depth == null) {
      // An unreachable broker is not an empty queue. Hold, and do not feed the estimator a gap.
      return;
    }

    lastObservedDepth.set(depth);
    latencyEstimator.sample(depth, ingestMetrics.acceptedCount(), nanoTime.getAsLong());

    int currentRate = rateLimiter.getCurrentRate();
    Zone zone =
        config.getSetpointMode() == SkierProperties.SetpointMode.LATENCY
            ? latencyZone(latencyEstimator.latencySeconds())
            : depthZone(depth);

    int instances = fleet.size();
    switch (zone) {
        // SEVERE is not divided: halving is scale-free, and dividing it gives a 1/2^N collapse.
      case SEVERE -> shed(currentRate, currentRate / 2, depth);
      case ABOVE ->
          shed(currentRate, currentRate - FleetRegistry.share(DECREASE_STEP, instances), depth);
      case BELOW ->
          rateLimiter.adjustRateUp(
              Math.min(
                  currentRate + FleetRegistry.share(INCREASE_STEP, instances), Constants.MAX_RATE));
      case HOLD -> {}
    }

    int newRate = rateLimiter.getCurrentRate();
    if (newRate != currentRate && logger.isDebugEnabled()) {
      logger.debug(
          "Queue depth {} (latency {}s) -> admission rate {} -> {}",
          depth,
          latencyEstimator.latencySeconds(),
          currentRate,
          newRate);
    }
  }

  /**
   * Applies a decrease, and counts the ticks where there is no decrease left to apply. The
   * condition is "already at the floor and still asked to shed", not "the requested rate is below
   * the floor", which every healthy retreat that reaches the floor also satisfies.
   */
  private void shed(int currentRate, int desiredRate, int depth) {
    int floor = rateLimiter.effectiveFloor();
    if (currentRate <= floor) {
      double pinned = floorPinned.count() + 1;
      floorPinned.increment();
      // Decimated: once this starts it fires every tick, five lines a second at the default rate.
      if (pinned == 1 || pinned % 100 == 0) {
        logger.warn(
            "Admission pinned at its floor of {} permits/s (this instance's share of a configured"
                + " {} across {} instance(s)) on {} tick(s) while depth {} still calls for a"
                + " decrease (drain-rate estimate {} events/s). There is no shedding left. If this"
                + " persists the floor is above what the pipeline can drain, in which case the"
                + " backlog grows however hard the controller sheds and no setpoint fixes it -"
                + " lower skier.admission.min-rate below the observed drain rate.",
            floor,
            rateLimiter.configuredFloor(),
            fleet.size(),
            (long) pinned,
            depth,
            latencyEstimator.drainRatePerSecond());
      }
    }
    rateLimiter.adjustRateDown(Math.max(desiredRate, floor));
  }

  private static Zone depthZone(int depth) {
    if (depth >= Constants.MAX_QUEUE_SIZE) {
      return Zone.SEVERE;
    }
    if (depth > Constants.TARGET_QUEUE_SIZE) {
      return Zone.ABOVE;
    }
    return depth < Constants.MIN_QUEUE_SIZE ? Zone.BELOW : Zone.HOLD;
  }

  private Zone latencyZone(double seconds) {
    if (Double.isNaN(seconds)) {
      // Acting on a latency requires a drain rate, and there is not one yet.
      return Zone.HOLD;
    }
    // +Inf lands here by intent: a non-empty queue that is not draining is past every ceiling.
    if (seconds >= config.getMaxLatency().toNanos() / 1_000_000_000.0) {
      return Zone.SEVERE;
    }
    if (seconds > config.getTargetLatency().toNanos() / 1_000_000_000.0) {
      return Zone.ABOVE;
    }
    return seconds < config.getMinLatency().toNanos() / 1_000_000_000.0 ? Zone.BELOW : Zone.HOLD;
  }

  /** Current depth, or {@code null} if the broker could not be sampled. */
  private Integer sampleQueueDepth() {
    try {
      var info = rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE);
      consecutiveSampleFailures.set(0);
      return info == null ? null : info.getMessageCount();
    } catch (Exception e) {
      int failures = consecutiveSampleFailures.incrementAndGet();
      // Decimated: a broker outage otherwise emits one line per tick.
      if (failures == 1 || failures % 50 == 0) {
        logger.warn("Queue depth sample failed ({} consecutive): {}", failures, e.getMessage());
      }
      lastObservedDepth.set(-1);
      return null;
    }
  }

  /** Most recent sample, or -1 if unknown. */
  public int lastObservedDepth() {
    return lastObservedDepth.get();
  }

  /** {@code NaN} before the window fills, {@code +Inf} when not draining; set in both modes. */
  public double observedQueueLatencySeconds() {
    return latencyEstimator.latencySeconds();
  }

  public double observedDrainRatePerSecond() {
    return latencyEstimator.drainRatePerSecond();
  }
}
