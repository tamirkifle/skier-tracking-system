package client.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LatencyRecorderTest {

  private static long micros(long millis) {
    return millis * 1000;
  }

  @Test
  @DisplayName("an interval contains only what was recorded since the previous interval")
  void intervalsDoNotOverlap() {
    LatencyRecorder recorder = new LatencyRecorder();

    for (int i = 0; i < 100; i++) {
      recorder.record(micros(10), 201, System.nanoTime());
    }
    LatencyRecorder.Interval first = recorder.snapshot("phase-1");

    for (int i = 0; i < 50; i++) {
      recorder.record(micros(200), 201, System.nanoTime());
    }
    LatencyRecorder.Interval second = recorder.snapshot("phase-2");

    assertThat(first.latency().getTotalCount()).isEqualTo(100);
    assertThat(second.latency().getTotalCount()).isEqualTo(50);
    assertThat(first.latency().getMean())
        .isCloseTo(micros(10), org.assertj.core.data.Offset.offset(500d));
    assertThat(second.latency().getMean())
        .isCloseTo(micros(200), org.assertj.core.data.Offset.offset(2000d));
  }

  @Test
  @DisplayName("outcome counters are also scoped to the interval")
  void outcomeCountersAreScopedToTheInterval() {
    LatencyRecorder recorder = new LatencyRecorder();

    recorder.record(micros(5), 201, System.nanoTime());
    recorder.record(micros(5), 201, System.nanoTime());
    recorder.snapshot("phase-1");

    recorder.record(micros(5), 201, System.nanoTime());
    recorder.record(micros(5), 429, System.nanoTime());
    recorder.record(micros(5), 599, System.nanoTime());
    LatencyRecorder.Interval second = recorder.snapshot("phase-2");

    assertThat(second.successes()).isEqualTo(1);
    assertThat(second.failures()).isEqualTo(2);
    assertThat(second.completed()).isEqualTo(3);
    assertThat(second.statusCounts()).containsOnlyKeys(201, 429, 599);
    assertThat(second.statusCounts()).containsEntry(201, 1L).containsEntry(429, 1L);
  }

  @Test
  @DisplayName("2xx is success and everything else is failure")
  void classifiesOutcomes() {
    LatencyRecorder recorder = new LatencyRecorder();

    recorder.record(micros(1), 200, System.nanoTime());
    recorder.record(micros(1), 201, System.nanoTime());
    recorder.record(micros(1), 204, System.nanoTime());
    recorder.record(micros(1), 400, System.nanoTime());
    recorder.record(micros(1), 429, System.nanoTime());
    recorder.record(micros(1), 503, System.nanoTime());
    recorder.record(micros(1), 599, System.nanoTime());

    assertThat(recorder.successCount()).isEqualTo(3);
    assertThat(recorder.failureCount()).isEqualTo(4);
    assertThat(recorder.totalCount()).isEqualTo(7);
  }

  @Test
  @DisplayName("percentiles come from the full distribution, not a sample")
  void reportsAccuratePercentiles() {
    LatencyRecorder recorder = new LatencyRecorder();

    // 990 fast requests and 10 slow ones: p99 must land in the slow group.
    for (int i = 0; i < 990; i++) {
      recorder.record(micros(10), 201, System.nanoTime());
    }
    for (int i = 0; i < 10; i++) {
      recorder.record(micros(1000), 201, System.nanoTime());
    }

    var histogram = recorder.snapshot("all").latency();
    assertThat(histogram.getValueAtPercentile(50))
        .isCloseTo(micros(10), org.assertj.core.data.Offset.offset(micros(1)));
    assertThat(histogram.getValueAtPercentile(99.5)).isGreaterThan(micros(500));
    assertThat(histogram.getMaxValue())
        .isCloseTo(micros(1000), org.assertj.core.data.Offset.offset(micros(2)));
  }

  @Test
  @DisplayName("recording is safe and lossless under heavy concurrency")
  void isThreadSafe() throws Exception {
    LatencyRecorder recorder = new LatencyRecorder();
    int threads = 16;
    int perThread = 5000;

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      tasks.add(
          () -> {
            for (int i = 0; i < perThread; i++) {
              recorder.record(micros(1 + (i % 50)), 201, System.nanoTime());
            }
            return null;
          });
    }
    for (Future<Void> future : pool.invokeAll(tasks)) {
      future.get();
    }
    pool.shutdownNow();

    assertThat(recorder.totalCount()).isEqualTo((long) threads * perThread);
    assertThat(recorder.snapshot("all").latency().getTotalCount())
        .isEqualTo((long) threads * perThread);
  }

  @Test
  @DisplayName("a value beyond the tracking ceiling is clamped rather than dropped")
  void clampsRatherThanDrops() {
    LatencyRecorder recorder = new LatencyRecorder();
    recorder.record(Long.MAX_VALUE, 598, System.nanoTime());

    assertThat(recorder.snapshot("all").latency().getTotalCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("retries are tracked as attempts, separately from requests")
  void tracksRetriesSeparately() {
    LatencyRecorder recorder = new LatencyRecorder();

    recorder.recordRetry();
    recorder.recordRetry();
    recorder.record(micros(30), 201, System.nanoTime());

    assertThat(recorder.retryCount()).isEqualTo(2);
    assertThat(recorder.totalCount()).isEqualTo(1);
  }
}
