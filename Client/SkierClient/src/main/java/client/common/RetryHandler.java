package client.common;



import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class RetryHandler {
  private static final int MAX_RETRIES = 5;
  private static final long INITIAL_BACKOFF_MS = 100; // Start with 100ms
  private static final long MAX_BACKOFF_MS = 5000; // Cap at 5 seconds

  public static <T> T executeWithRetry(Supplier<T> task) {
    int attempts = 0;
    long backoff = INITIAL_BACKOFF_MS;

    while (attempts < MAX_RETRIES) {
      try {
        return task.get();
      } catch (Exception e) {
        attempts++;
        if (attempts >= MAX_RETRIES) {
          throw new RuntimeException("Max retries exceeded", e);
        }

        try {
          TimeUnit.MILLISECONDS.sleep(backoff);
          backoff = Math.min(backoff * 2, MAX_BACKOFF_MS); // Exponential backoff
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted during retry wait", ie);
        }
      }
    }
    throw new RuntimeException("Unexpected exit from retry loop");
  }
}
