package client.transport;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A shared daemon timer, so backoff never sleeps an HTTP client IO reactor thread. */
final class RetryScheduler {

  private static final ScheduledExecutorService SCHEDULER =
      Executors.newScheduledThreadPool(
          2,
          runnable -> {
            Thread thread = new Thread(runnable, "retry-timer");
            thread.setDaemon(true);
            return thread;
          });

  private RetryScheduler() {}

  static void schedule(Runnable task, long delayMs) {
    SCHEDULER.schedule(task, delayMs, TimeUnit.MILLISECONDS);
  }

  static void shutdown() {
    SCHEDULER.shutdownNow();
  }
}
