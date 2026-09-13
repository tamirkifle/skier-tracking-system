package skiers.service;

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

  private final RabbitAdmin rabbitAdmin;
  private final RateLimiter rateLimiter;
  private final IngestMetrics ingestMetrics;
  private final SkierProperties.QueueMonitor config;
  private final FleetRegistry fleet;
  private final QueueLatencyEstimator latencyEstimator;
  private final LongSupplier nanoTime;
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
