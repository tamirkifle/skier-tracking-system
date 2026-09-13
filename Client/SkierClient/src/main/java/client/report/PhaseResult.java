package client.report;

import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;

/** What one phase did. Every field is measured, not derived from what the scenario asked for. */
public record PhaseResult(
    String name,
    boolean warmup,
    String mode,
    long requestsIssued,
    long requestsCompleted,
    long successes,
    long failures,
    long retries,
    double wallSeconds,
    double dispatchSeconds,
    long lateDispatches,
    long maxLatenessMillis,
    Histogram latency,
    List<Integer> concurrencySamples,
    Map<Integer, Long> statusCounts,
    long targetRatePerSecond) {

  public double throughput() {
    return wallSeconds <= 0 ? 0 : requestsCompleted / wallSeconds;
  }

  public double meanConcurrency() {
    return concurrencySamples.isEmpty()
        ? 0
        : concurrencySamples.stream().mapToInt(Integer::intValue).average().orElse(0);
  }

  public double littlesLawThroughput() {
    double meanLatencySeconds = latency.getMean() / 1_000_000d;
    return meanLatencySeconds <= 0 ? 0 : meanConcurrency() / meanLatencySeconds;
  }

  public double littlesLawDivergence() {
    double measured = throughput();
    return measured <= 0 ? 0 : Math.abs(littlesLawThroughput() - measured) / measured;
  }

  public double successRate() {
    return requestsCompleted == 0 ? 0 : (double) successes / requestsCompleted;
  }

  public double rateAttainment() {
    if (targetRatePerSecond <= 0 || dispatchSeconds <= 0) {
      return 1.0;
    }
    return (requestsIssued / dispatchSeconds) / targetRatePerSecond;
  }

  public double lateDispatchRate() {
    return requestsIssued == 0 ? 0 : (double) lateDispatches / requestsIssued;
  }

  public long p50Micros() {
    return latency.getValueAtPercentile(50);
  }

  public long p95Micros() {
    return latency.getValueAtPercentile(95);
  }

  public long p99Micros() {
    return latency.getValueAtPercentile(99);
  }

  public long p999Micros() {
    return latency.getValueAtPercentile(99.9);
  }
}
