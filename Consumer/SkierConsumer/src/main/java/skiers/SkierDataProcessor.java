package skiers;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import skiers.cardinality.UniqueSkierCounter;
import skiers.config.ConsumerProperties;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import skiers.persistence.LiftRideWriter;

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

  private static final long IDLE_POLL_MS = 100;

  private final BlockingQueue<LiftRideEvent> stagingQueue;
  private final LiftRideWriter writer;
  private final UniqueSkierCounter uniqueSkierCounter;
  private final ConsumerMetrics metrics;
  private final ConsumerProperties.Writer config;

  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Thread> writerThreads = new ArrayList<>();
  private final CountDownLatch writersStopped;
  private final AtomicInteger inFlight = new AtomicInteger();

  public SkierDataProcessor(
      LiftRideWriter writer,
      UniqueSkierCounter uniqueSkierCounter,
      ConsumerMetrics metrics,
      ConsumerProperties properties,
      MeterRegistry registry) {
    this.writer = writer;
    this.uniqueSkierCounter = uniqueSkierCounter;
    this.metrics = metrics;
    this.config = properties.getWriter();
    this.stagingQueue = new LinkedBlockingQueue<>(config.getQueueCapacity());
    this.writersStopped = new CountDownLatch(config.getThreads());

    metrics.bindQueueDepth(registry, "skier.write.staging.depth", stagingQueue::size);
    metrics.bindQueueDepth(registry, "skier.write.inflight", inFlight::get);

    startWriters();

    logger.info(
        "Write pipeline up: writer={}, threads={}, batchSize={}, lingerMs={}, queueCapacity={}, "
            + "cardinality={}",
        writer.name(),
        config.getThreads(),
        effectiveBatchSize(),
        config.getLingerMs(),
        config.getQueueCapacity(),
        uniqueSkierCounter.name());
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

  private int effectiveBatchSize() {
    return Math.max(1, Math.min(config.getBatchSize(), writer.maxBatchSize()));
  }

  private void startWriters() {
    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("dynamo-writer-" + thread.getId());
          thread.setDaemon(true);
          return thread;
        };

    for (int i = 0; i < config.getThreads(); i++) {
      Thread thread = threadFactory.newThread(this::writeLoop);
      thread.start();
      writerThreads.add(thread);
    }
  }

  private void writeLoop() {
    int batchSize = effectiveBatchSize();
    List<LiftRideEvent> batch = new ArrayList<>(batchSize);

    try {
      while (running.get() || !stagingQueue.isEmpty()) {
        if (!fillBatch(batch, batchSize)) {
          continue;
        }
        inFlight.addAndGet(batch.size());
        try {
          flush(batch);
        } finally {
          inFlight.addAndGet(-batch.size());
          // flush() settles every event it is handed, so once it returns the list holds nothing
          // this thread still owns. Leaving them would reject events that are already durable.
          batch.clear();
          metrics.queueDepth(stagingQueue.size());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // Non-empty only if fillBatch was interrupted mid-accumulation; these are still the broker's.
      batch.forEach(event -> event.ack().reject(true));
      writersStopped.countDown();
    }
  }

  /** The linger starts once the first event is in hand, so an idle burst pays no extra latency. */
  private boolean fillBatch(List<LiftRideEvent> batch, int batchSize) throws InterruptedException {
    LiftRideEvent first = stagingQueue.poll(IDLE_POLL_MS, TimeUnit.MILLISECONDS);
    if (first == null) {
      return false;
    }
    batch.add(first);

    if (batchSize == 1) {
      return true;
    }

    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getLingerMs());
    while (batch.size() < batchSize) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      LiftRideEvent next = stagingQueue.poll(remaining, TimeUnit.NANOSECONDS);
      if (next == null) {
        break;
      }
      batch.add(next);
    }
    return true;
  }

  private void flush(List<LiftRideEvent> batch) {
    long startedAt = System.nanoTime();
    List<LiftRideEvent> toRetry;

    try {
      toRetry = writer.write(batch);
    } catch (Exception e) {
      logger.warn(
          "Write request of {} item(s) failed: {}: {}",
          batch.size(),
          e.getClass().getSimpleName(),
          e.getMessage());
      batch.forEach(this::retryOrDeadLetter);
      return;
    }

    int persisted = batch.size() - toRetry.size();
    if (persisted > 0) {
      metrics.recordWritten(persisted, System.nanoTime() - startedAt);
    }

    // One clock read for the group: everything the writer did not defer landed in one request.
    long durableAt = System.currentTimeMillis();

    for (LiftRideEvent event : batch) {
      // LiftRideEvent does not override equals, so this is identity membership: two deliveries of
      // one ride settle independently. Linear scan is bounded by the batch size.
      if (toRetry.contains(event)) {
        retryOrDeadLetter(event);
        continue;
      }
      if (!updateCardinality(event)) {
        retryOrDeadLetter(event);
        continue;
      }
      metrics.recordFreshness(event.publishedAtMillis(), durableAt);
      event.ack().ack();
    }
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

  private void retryOrDeadLetter(LiftRideEvent event) {
    int attempt = event.recordAttempt();
    if (attempt > config.getMaxRetries()) {
      metrics.recordDropped();
      logger.error(
          "Dead-lettering event after {} attempts: skier={} sortKey={}",
          attempt,
          event.skierId(),
          event.sortKey());
      event.ack().reject(false);
      return;
    }

    metrics.recordRetry();
    if (!event.ack().retryLater(attempt)) {
      // The delay route refused it, so the delivery went back unchanged and the attempt just spent
      // is recorded nowhere. Counted apart from retries: this is where the budget stops binding.
      metrics.recordRetryHandoffFailed();
    }
  }

  public int stagedCount() {
    return stagingQueue.size();
  }

  public boolean isRunning() {
    return running.get();
  }
}
