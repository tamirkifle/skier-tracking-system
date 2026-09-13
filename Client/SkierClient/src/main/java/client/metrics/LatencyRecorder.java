package client.metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Latency capture for a load run. An HdrHistogram {@link Recorder} keeps per-thread histograms, so
 * the recording path is lock-free and the tool does not measure its own contention.
 */
public final class LatencyRecorder {

  /** One hour at three significant digits. A value above the ceiling is recorded at it. */
  private static final long MAX_TRACKABLE_MICROS = 3_600_000_000L;

  private static final int SIGNIFICANT_DIGITS = 3;

  private final Recorder recorder = new Recorder(MAX_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);
  private final Histogram cumulative = new Histogram(MAX_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);

  private final LongAdder successes = new LongAdder();
  private final LongAdder failures = new LongAdder();
  private final LongAdder retries = new LongAdder();
  private final ConcurrentHashMap<Integer, LongAdder> byStatus = new ConcurrentHashMap<>();

  // Counter values at the previous snapshot, so an interval is derived by subtraction.
  private long baselineSuccesses;
  private long baselineFailures;
  private long baselineRetries;
  private Map<Integer, Long> baselineStatuses = Map.of();

  private final AtomicLong firstRecordedAtNanos = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong lastRecordedAtNanos = new AtomicLong(Long.MIN_VALUE);

  public void record(long serviceTimeMicros, int status, long completedAtNanos) {
    recorder.recordValue(Math.min(Math.max(serviceTimeMicros, 0), MAX_TRACKABLE_MICROS));

    if (status >= 200 && status < 300) {
      successes.increment();
    } else {
      failures.increment();
    }
    byStatus.computeIfAbsent(status, key -> new LongAdder()).increment();

    firstRecordedAtNanos.accumulateAndGet(completedAtNanos, Math::min);
    lastRecordedAtNanos.accumulateAndGet(completedAtNanos, Math::max);
  }

  public void recordRetry() {
    retries.increment();
  }

  /**
   * Each call drains the recorder, so two consecutive snapshots never overlap. The drain and the
   * counter reads are not atomic, so a request completing between them lands in this interval's
   * counters and the next interval's histogram.
   */
  public synchronized Interval snapshot(String name) {
    Histogram histogram = recorder.getIntervalHistogram();
    cumulative.add(histogram);

    long successNow = successes.sum();
    long failureNow = failures.sum();
    long retryNow = retries.sum();
    Map<Integer, Long> statusNow = currentStatusCounts();

    Interval interval =
        new Interval(
            name,
            histogram,
            successNow - baselineSuccesses,
            failureNow - baselineFailures,
            retryNow - baselineRetries,
            difference(statusNow, baselineStatuses));

    baselineSuccesses = successNow;
    baselineFailures = failureNow;
    baselineRetries = retryNow;
    baselineStatuses = statusNow;

    return interval;
  }

  public synchronized Histogram cumulativeHistogram() {
    cumulative.add(recorder.getIntervalHistogram());
    return cumulative.copy();
  }

  public long successCount() {
    return successes.sum();
  }

  public long failureCount() {
    return failures.sum();
  }

  public long retryCount() {
    return retries.sum();
  }

  public long totalCount() {
    return successes.sum() + failures.sum();
  }

  public Map<Integer, Long> statusCounts() {
    return currentStatusCounts();
  }

  private Map<Integer, Long> currentStatusCounts() {
    Map<Integer, Long> counts = new TreeMap<>();
    byStatus.forEach((status, count) -> counts.put(status, count.sum()));
    return counts;
  }

  private static Map<Integer, Long> difference(Map<Integer, Long> now, Map<Integer, Long> before) {
    Map<Integer, Long> delta = new TreeMap<>();
    now.forEach(
        (status, count) -> {
          long change = count - before.getOrDefault(status, 0L);
          if (change > 0) {
            delta.put(status, change);
          }
        });
    return delta;
  }

  public record Interval(
      String name,
      Histogram latency,
      long successes,
      long failures,
      long retries,
      Map<Integer, Long> statusCounts) {

    public long completed() {
      return successes + failures;
    }
  }

  public double observedWindowSeconds() {
    long first = firstRecordedAtNanos.get();
    long last = lastRecordedAtNanos.get();
    if (first == Long.MAX_VALUE || last <= first) {
      return 0d;
    }
    return (last - first) / 1_000_000_000d;
  }
}
