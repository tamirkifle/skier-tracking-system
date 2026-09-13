package skiers.support;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import skiers.ingest.DeliveryAck;

/** Records how a delivery was settled. Deliberately not idempotent, so double settlement shows. */
public final class RecordingAck implements DeliveryAck {

  private final AtomicInteger acks = new AtomicInteger();
  private final AtomicInteger rejectsWithRequeue = new AtomicInteger();
  private final AtomicInteger rejectsWithoutRequeue = new AtomicInteger();
  private final AtomicInteger delayedRetries = new AtomicInteger();
  private final AtomicInteger lastAttempt = new AtomicInteger(-1);
  private final CountDownLatch settled = new CountDownLatch(1);

  private volatile boolean retryRouteAvailable = true;

  @Override
  public void ack() {
    acks.incrementAndGet();
    settled.countDown();
  }

  @Override
  public void reject(boolean requeue) {
    if (requeue) {
      rejectsWithRequeue.incrementAndGet();
    } else {
      rejectsWithoutRequeue.incrementAndGet();
    }
    settled.countDown();
  }

  @Override
  public boolean retryLater(int attempt) {
    lastAttempt.set(attempt);
    if (!retryRouteAvailable) {
      reject(true);
      return false;
    }
    delayedRetries.incrementAndGet();
    settled.countDown();
    return true;
  }

  public void breakRetryRoute() {
    retryRouteAvailable = false;
  }

  public int lastAttempt() {
    return lastAttempt.get();
  }

  public int delayedRetries() {
    return delayedRetries.get();
  }

  public boolean awaitSettlement(long millis) throws InterruptedException {
    return settled.await(millis, java.util.concurrent.TimeUnit.MILLISECONDS);
  }

  public int acks() {
    return acks.get();
  }

  public int requeues() {
    return rejectsWithRequeue.get();
  }

  public int deadLetters() {
    return rejectsWithoutRequeue.get();
  }

  public boolean isSettled() {
    return settled.getCount() == 0;
  }
}
