package skiers;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import skiers.cardinality.UniqueSkierCounter;
import skiers.config.ConsumerProperties;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;

/**
 * The write pipeline: a bounded staging queue drained by a fixed pool of writer threads.
 *
 * <p>Settlement follows durability; the right to ack travels with the event. A full staging queue
 * refuses on the listener thread rather than growing. Retry delay belongs to the broker, not to a
 * writer thread. Shutdown drains, then returns the remainder.
 */
@Service
public class SkierDataProcessor {

  private static final Logger logger = LoggerFactory.getLogger(SkierDataProcessor.class);

  private final BlockingQueue<LiftRideEvent> stagingQueue;
  private final UniqueSkierCounter uniqueSkierCounter;
  private final ConsumerMetrics metrics;
  private final ConsumerProperties.Writer config;

  private final AtomicBoolean running = new AtomicBoolean(true);

  public SkierDataProcessor(
      UniqueSkierCounter uniqueSkierCounter,
      ConsumerMetrics metrics,
      ConsumerProperties properties,
      MeterRegistry registry) {
    this.uniqueSkierCounter = uniqueSkierCounter;
    this.metrics = metrics;
    this.config = properties.getWriter();
    this.stagingQueue = new LinkedBlockingQueue<>(config.getQueueCapacity());

    metrics.bindQueueDepth(registry, "skier.write.staging.depth", stagingQueue::size);
  }

  public boolean submit(LiftRideEvent event) {
    if (!running.get()) {
      event.ack().reject(true);
      metrics.recordRequeued();
      return false;
    }
    if (!stagingQueue.offer(event)) {
      event.ack().reject(true);
      metrics.recordRequeued();
      return false;
    }
    metrics.queueDepth(stagingQueue.size());
    return true;
  }

  /**
   * Updates the derived unique-skier count, and reports whether the delivery may be settled.
   *
   * <p>An exact count that misses an increment cannot repair itself, so the delivery is retried. An
   * estimate self-corrects on the next sighting, so the ride settles and the failure is logged.
   */
  private boolean updateCardinality(LiftRideEvent event) {
    try {
      uniqueSkierCounter.observe(event);
      return true;
    } catch (RuntimeException e) {
      logger.warn("Cardinality update failed for {}: {}", event.skierId(), e.getMessage());
      if (!uniqueSkierCounter.projectionRequired()) {
        return true;
      }
      metrics.recordProjectionUnresolved();
      return false;
    }
  }

  public int stagedCount() {
    return stagingQueue.size();
  }

  public boolean isRunning() {
    return running.get();
  }
}
