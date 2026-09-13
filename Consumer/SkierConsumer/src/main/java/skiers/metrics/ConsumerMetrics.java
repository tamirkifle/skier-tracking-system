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
  private final Timer writeLatency;
  private final DistributionSummary batchSize;

  public ConsumerMetrics(MeterRegistry registry) {
    this.written =
        Counter.builder("skier.write.total")
            .description("Lift-ride items durably persisted to DynamoDB")
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

  public long writtenCount() {
    return (long) written.count();
  }
}
