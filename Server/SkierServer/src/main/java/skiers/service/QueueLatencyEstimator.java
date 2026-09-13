package skiers.service;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/** Turns queue depth into queue latency: how long an event joining the queue now waits. */
public final class QueueLatencyEstimator {

  private static final int MAX_SAMPLES = 1024;

  private final long windowNanos;
  private final Deque<Sample> window = new ArrayDeque<>();
  private final AtomicLong unresolved = new AtomicLong();

  private volatile double drainRatePerSecond = Double.NaN;
  private volatile double latencySeconds = Double.NaN;

  public QueueLatencyEstimator(Duration window) {
    this.windowNanos = window.toNanos();
  }

  public void sample(int depth, double publishedTotal, long nanoTime) {
    Sample newest = new Sample(nanoTime, depth, publishedTotal);
    Sample previous = window.peekLast();
    if (previous != null && nanoTime <= previous.nanoTime()) {
      // A repeated timestamp divides by zero and a backwards one yields a negative rate.
      window.clear();
      window.addLast(newest);
      unresolve();
      return;
    }

    window.addLast(newest);
    prune(nanoTime);

    Sample base = window.peekFirst();
    long elapsedNanos = nanoTime - base.nanoTime();
    if (elapsedNanos < windowNanos) {
      unresolve();
      return;
    }

    double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    double publishedDelta = publishedTotal - base.published();
    double depthDelta = (double) depth - base.depth();
    // Negative means another producer grew the queue, and a negative drain rate is not a slower
    // consumer but an unmeasurable one.
    double drained = Math.max(publishedDelta - depthDelta, 0.0);

    drainRatePerSecond = drained / elapsedSeconds;
    if (depth == 0) {
      latencySeconds = 0.0;
    } else if (drainRatePerSecond <= 0.0) {
      latencySeconds = Double.POSITIVE_INFINITY;
    } else {
      latencySeconds = depth / drainRatePerSecond;
    }
  }

  private void prune(long nanoTime) {
    while (window.size() > 1) {
      Iterator<Sample> oldestFirst = window.iterator();
      oldestFirst.next();
      Sample candidate = oldestFirst.next();
      if (nanoTime - candidate.nanoTime() < windowNanos) {
        break;
      }
      window.removeFirst();
    }
    while (window.size() > MAX_SAMPLES) {
      window.removeFirst();
    }
  }

  private void unresolve() {
    drainRatePerSecond = Double.NaN;
    latencySeconds = Double.NaN;
    unresolved.incrementAndGet();
  }

  /** Events/second over the window, or {@code NaN} if it could not be computed. */
  public double drainRatePerSecond() {
    return drainRatePerSecond;
  }

  /** {@code NaN} means no estimate yet, {@code 0} an empty queue, {@code +Inf} a stalled one. */
  public double latencySeconds() {
    return latencySeconds;
  }

  /** Samples with no computable drain rate, so holding is distinguishable from steering. */
  public long unresolvedSamples() {
    return unresolved.get();
  }

  private record Sample(long nanoTime, int depth, double published) {}
}
