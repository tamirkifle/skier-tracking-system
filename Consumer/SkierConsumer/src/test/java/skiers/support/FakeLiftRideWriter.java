package skiers.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import skiers.model.LiftRideEvent;
import skiers.persistence.LiftRideWriter;

/** Scriptable {@link LiftRideWriter} that records the groups it receives. */
public final class FakeLiftRideWriter implements LiftRideWriter {

  private final List<List<LiftRideEvent>> batches = new CopyOnWriteArrayList<>();
  private final AtomicInteger itemsSeen = new AtomicInteger();
  private final AtomicReference<Exception> failWith = new AtomicReference<>();
  private final AtomicReference<Function<List<LiftRideEvent>, List<LiftRideEvent>>> partialFailure =
      new AtomicReference<>(events -> List.of());
  private final int maxBatchSize;
  private final AtomicReference<CountDownLatch> gate = new AtomicReference<>();

  public FakeLiftRideWriter() {
    this(25);
  }

  public FakeLiftRideWriter(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public void failEveryWriteWith(Exception exception) {
    failWith.set(exception);
  }

  public void succeedFromNowOn() {
    failWith.set(null);
  }

  public void deferItems(Function<List<LiftRideEvent>, List<LiftRideEvent>> selector) {
    partialFailure.set(selector);
  }

  public CountDownLatch blockWrites() {
    CountDownLatch latch = new CountDownLatch(1);
    gate.set(latch);
    return latch;
  }

  public void release() {
    CountDownLatch latch = gate.getAndSet(null);
    if (latch != null) {
      latch.countDown();
    }
  }

  @Override
  public List<LiftRideEvent> write(List<LiftRideEvent> events) throws Exception {
    CountDownLatch latch = gate.get();
    if (latch != null) {
      latch.await();
    }

    batches.add(new ArrayList<>(events));
    itemsSeen.addAndGet(events.size());

    Exception failure = failWith.get();
    if (failure != null) {
      throw failure;
    }
    return partialFailure.get().apply(events);
  }

  @Override
  public String name() {
    return "fake";
  }

  @Override
  public int maxBatchSize() {
    return maxBatchSize;
  }

  public List<List<LiftRideEvent>> batches() {
    return Collections.unmodifiableList(batches);
  }

  public int itemsSeen() {
    return itemsSeen.get();
  }

  public int writeCallCount() {
    return batches.size();
  }
}
