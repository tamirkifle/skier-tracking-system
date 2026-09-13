package skiers.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/** Consumer-side instrumentation, exported over {@code /actuator/prometheus}. */
@Component
public class ConsumerMetrics {

  private final Counter written;
  private final Counter retried;
  private final Counter dropped;
  private final Counter requeued;
  private final Counter retryHandoffFailed;
  private final Counter projectionUnresolved;
  private final Counter duplicateSuppressed;
  private final Counter uniqueSkiersObserved;
  private final Counter cardinalityWriteRequests;
  private final Counter cardinalityWriteItems;
  private final Timer writeLatency;
  private final DistributionSummary batchSize;

  public ConsumerMetrics(MeterRegistry registry) {
    this.written =
        Counter.builder("skier.write.total")
            .description("Lift-ride items durably persisted to DynamoDB")
            .register(registry);
    this.retried =
        Counter.builder("skier.write.retries")
            .description("Write attempts that failed transiently and were retried")
            .register(registry);
    this.dropped =
        Counter.builder("skier.write.dropped")
            .description("Events dead-lettered after exhausting the retry budget")
            .register(registry);
    this.requeued =
        Counter.builder("skier.write.requeued")
            .description("Events returned to the broker because the writer could not accept them")
            .register(registry);
    this.retryHandoffFailed =
        Counter.builder("skier.write.retry.handoff.failed")
            .description(
                "Retries the delay route would not accept, so the delivery was requeued and its "
                    + "attempt was not recorded")
            .register(registry);
    this.projectionUnresolved =
        Counter.builder("skier.cardinality.projection.unresolved")
            .description(
                "Durable rides whose required unique-skier update failed, so the delivery was not "
                    + "settled")
            .register(registry);
    this.duplicateSuppressed =
        Counter.builder("skier.cardinality.duplicate.suppressed")
            .description("Redeliveries recognised as already-counted skier sightings")
            .register(registry);
    this.uniqueSkiersObserved =
        Counter.builder("skier.cardinality.unique.observed")
            .description("First sightings of a skier at a resort on a day")
            .register(registry);
    this.cardinalityWriteRequests =
        Counter.builder("skier.cardinality.write.requests")
            .description("DynamoDB write requests issued by the cardinality strategy")
            .register(registry);
    this.cardinalityWriteItems =
        Counter.builder("skier.cardinality.write.items")
            .description("DynamoDB items written by the cardinality strategy")
            .register(registry);
    this.writeLatency =
        Timer.builder("skier.write.latency")
            .description("Time from dequeue to durable write, per flush")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    this.batchSize =
        DistributionSummary.builder("skier.write.batch.size")
            .description("Items per DynamoDB write request")
            .register(registry);
  }

  public void recordWritten(int items, long nanos) {
    written.increment(items);
    batchSize.record(items);
    writeLatency.record(nanos, java.util.concurrent.TimeUnit.NANOSECONDS);
  }

  public void recordRetry() {
    retried.increment();
  }

  public void recordDropped() {
    dropped.increment();
  }

  public void recordRetryHandoffFailed() {
    retryHandoffFailed.increment();
  }

  public void recordProjectionUnresolved() {
    projectionUnresolved.increment();
  }

  public void recordRequeued() {
    requeued.increment();
  }

  public void recordUniqueSkier() {
    uniqueSkiersObserved.increment();
  }

  public void recordDuplicateSuppressed() {
    duplicateSuppressed.increment();
  }

  public void recordCardinalityWriteItems(int items) {
    cardinalityWriteItems.increment(items);
  }

  /** Counts requests issued: a conditional write that loses its condition is still billed. */
  public void recordCardinalityWriteRequest() {
    cardinalityWriteRequests.increment();
  }

  public long writtenCount() {
    return (long) written.count();
  }

  public long droppedCount() {
    return (long) dropped.count();
  }

  public long retryCount() {
    return (long) retried.count();
  }
}
