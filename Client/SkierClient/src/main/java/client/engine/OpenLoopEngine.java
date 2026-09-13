package client.engine;

import client.generator.EventFactory;
import client.metrics.LatencyRecorder;
import client.scenario.Scenario;
import client.transport.HttpTransport;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OpenLoopEngine implements LoadEngine {

  private static final Logger log = LoggerFactory.getLogger(OpenLoopEngine.class);

  private static final long SPIN_THRESHOLD_NANOS = TimeUnit.MICROSECONDS.toNanos(500);

  @Override
  public Dispatch run(
      Scenario.Phase phase,
      EventFactory events,
      HttpTransport transport,
      LatencyRecorder recorder,
      ProgressSink progress)
      throws InterruptedException {

    long targetRate = phase.getTargetRatePerSecond();
    if (targetRate <= 0) {
      throw new IllegalArgumentException(
          "Phase '" + phase.getName() + "' is open-loop but has no targetRatePerSecond");
    }

    long totalRequests = targetRate * phase.getDurationSeconds();
    long intervalNanos = TimeUnit.SECONDS.toNanos(1) / targetRate;
    AtomicLong issued = new AtomicLong();
    AtomicLong lateDispatches = new AtomicLong();
    AtomicLong maxLatenessNanos = new AtomicLong();

    log.info(
        "Phase '{}': open loop, {} req/s for {}s = {} requests (one every {} us)",
        phase.getName(),
        targetRate,
        phase.getDurationSeconds(),
        totalRequests,
        TimeUnit.NANOSECONDS.toMicros(intervalNanos));

    ClosedLoopEngine.ConcurrencySampler sampler =
        ClosedLoopEngine.ConcurrencySampler.start(transport, progress);

    long startNanos = System.nanoTime();
    long dispatchNanos = 0;
    try {
      for (long n = 0; n < totalRequests; n++) {
        long dueAtNanos = startNanos + n * intervalNanos;

        long lateness = System.nanoTime() - dueAtNanos;
        if (lateness > 0) {
          lateDispatches.incrementAndGet();
          maxLatenessNanos.accumulateAndGet(lateness, Math::max);
        } else {
          awaitUntil(dueAtNanos);
        }

        transport.send(events.next(), dueAtNanos);
        issued.incrementAndGet();
      }

      dispatchNanos = System.nanoTime() - startNanos;

      long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (transport.inFlight() > 0 && System.nanoTime() < drainDeadline) {
        TimeUnit.MILLISECONDS.sleep(20);
      }
    } finally {
      sampler.stop();
    }

    long late = lateDispatches.get();
    if (late > 0) {
      log.warn(
          "Phase '{}': {} of {} requests ({}%) were dispatched late, worst by {} ms. The client could "
              + "not sustain {} req/s, latencies include that shortfall by design, but the achieved "
              + "arrival rate was below target.",
          phase.getName(),
          late,
          issued.get(),
          String.format("%.1f", 100.0 * late / Math.max(1, issued.get())),
          TimeUnit.NANOSECONDS.toMillis(maxLatenessNanos.get()),
          targetRate);
    }

    return new Dispatch(
        issued.get(),
        dispatchNanos / 1_000_000_000d,
        late,
        TimeUnit.NANOSECONDS.toMillis(maxLatenessNanos.get()));
  }

  private static void awaitUntil(long deadlineNanos) throws InterruptedException {
    long remaining = deadlineNanos - System.nanoTime();
    if (remaining > SPIN_THRESHOLD_NANOS) {
      TimeUnit.NANOSECONDS.sleep(remaining - SPIN_THRESHOLD_NANOS);
    }
    while (System.nanoTime() < deadlineNanos) {
      Thread.onSpinWait();
    }
  }
}
