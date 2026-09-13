package skiers.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

@Component
public class IngestMetrics {

  private final Counter accepted;
  private final Counter shed;
  private final Counter invalid;
  private final Counter publishFailed;
  private final Counter publishUnknown;
  private final Timer publishLatency;
  private final Timer admissionWait;

  public IngestMetrics(MeterRegistry registry) {
    this.accepted =
        Counter.builder("skier.ingest.accepted")
            .description("Events admitted and published to the broker")
            .register(registry);
    this.shed =
        Counter.builder("skier.ingest.shed")
            .description("Events rejected with 429 because no permit was available in time")
            .register(registry);
    this.invalid =
        Counter.builder("skier.ingest.invalid")
            .description("Events rejected with 400 for failing input validation")
            .register(registry);
    this.publishFailed =
        Counter.builder("skier.ingest.publish.failed")
            .description("Events admitted but not publishable to the broker")
            .register(registry);
    // Separate from publish.failed: that says not queued, this says nobody knows.
    this.publishUnknown =
        Counter.builder("skier.ingest.publish.unknown")
            .description(
                "Events whose publish was neither confirmed nor refused before the timeout")
            .register(registry);
    this.publishLatency =
        Timer.builder("skier.ingest.publish.latency")
            // Confirmed publishes only; a confirm timeout lands in skier.ingest.publish.unknown.
            .description("Time spent publishing to RabbitMQ and awaiting its confirm")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    this.admissionWait =
        Timer.builder("skier.ingest.admission.wait")
            .description("Time a request spent waiting for an admission permit")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
  }

  public void recordAccepted(long publishNanos) {
    accepted.increment();
    publishLatency.record(publishNanos, TimeUnit.NANOSECONDS);
  }

  public void recordShed(long waitNanos) {
    shed.increment();
    admissionWait.record(waitNanos, TimeUnit.NANOSECONDS);
  }

  public void recordAdmissionWait(long waitNanos) {
    admissionWait.record(waitNanos, TimeUnit.NANOSECONDS);
  }

  public void recordInvalid() {
    invalid.increment();
  }

  public void recordPublishFailure() {
    publishFailed.increment();
  }

  public void recordPublishUnknown() {
    publishUnknown.increment();
  }

  public double acceptedCount() {
    return accepted.count();
  }

  public double shedCount() {
    return shed.count();
  }
}
