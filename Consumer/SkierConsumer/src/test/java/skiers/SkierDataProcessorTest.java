package skiers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import skiers.cardinality.UniqueSkierCounter;
import skiers.config.ConsumerProperties;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import skiers.support.FakeLiftRideWriter;
import skiers.support.RecordingAck;

class SkierDataProcessorTest {

  private SkierDataProcessor processor;
  private FakeLiftRideWriter writer;
  private CountingCounter counter;
  private SimpleMeterRegistry registry;

  private static final class CountingCounter implements UniqueSkierCounter {
    private final AtomicInteger observations = new AtomicInteger();
    private volatile RuntimeException failure;
    private volatile boolean required;

    @Override
    public boolean observe(LiftRideEvent event) {
      if (failure != null) {
        throw failure;
      }
      observations.incrementAndGet();
      return true;
    }

    @Override
    public boolean projectionRequired() {
      return required;
    }

    @Override
    public String name() {
      return "counting";
    }
  }

  private SkierDataProcessor build(
      int threads, int batchSize, long lingerMs, int queueCapacity, int maxRetries) {
    writer = new FakeLiftRideWriter(batchSize);
    counter = new CountingCounter();

    ConsumerProperties properties = new ConsumerProperties();
    properties.getWriter().setThreads(threads);
    properties.getWriter().setBatchSize(batchSize);
    properties.getWriter().setLingerMs(lingerMs);
    properties.getWriter().setQueueCapacity(queueCapacity);
    properties.getWriter().setMaxRetries(maxRetries);

    registry = new SimpleMeterRegistry();
    processor =
        new SkierDataProcessor(
            writer, counter, new ConsumerMetrics(registry), properties, registry);
    return processor;
  }

  private static LiftRideEvent event(int skierId, int time, RecordingAck ack) {
    return event(skierId, time, ack, 0);
  }

  private static LiftRideEvent event(int skierId, int time, RecordingAck ack, int priorAttempts) {
    return new LiftRideEvent(
        "evt-" + skierId + '-' + time,
        String.valueOf(skierId),
        "5",
        "2025",
        "1",
        21,
        time,
        ack,
        null,
        priorAttempts);
  }

  private static LiftRideEvent stampedEvent(int skierId, long publishedAtMillis, RecordingAck ack) {
    return new LiftRideEvent(
        "evt-" + skierId,
        String.valueOf(skierId),
        "5",
        "2025",
        "1",
        21,
        100,
        ack,
        publishedAtMillis);
  }

  @AfterEach
  void tearDown() {
    if (processor != null) {
      processor.shutdown();
    }
  }

  @Test
  @DisplayName("a message is acknowledged only after the write returns")
  void acknowledgesOnlyAfterDurableWrite() throws Exception {
    build(1, 1, 0, 100, 3);
    CountDownLatch gate = writer.blockWrites();

    RecordingAck ack = new RecordingAck();
    assertThat(processor.submit(event(42, 100, ack))).isTrue();

    await()
        .during(Duration.ofMillis(150))
        .atMost(Duration.ofSeconds(2))
        .until(() -> !ack.isSettled());
    assertThat(ack.acks()).isZero();

    gate.countDown();
    writer.release();

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.acks() == 1);
    assertThat(ack.requeues()).isZero();
    assertThat(ack.deadLetters()).isZero();
  }

  @Test
  @DisplayName("cardinality is updated exactly once per durable event")
  void updatesCardinalityAfterWrite() {
    build(1, 1, 0, 100, 3);
    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.acks() == 1);
    assertThat(counter.observations.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("an optional cardinality failure does not replay a durable event")
  void optionalCardinalityFailureDoesNotReplayADurableEvent() {
    build(1, 1, 0, 100, 3);
    counter.required = false;
    counter.failure = new IllegalStateException("redis down");

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.acks() == 1);
    assertThat(ack.requeues()).isZero();
    assertThat(ack.delayedRetries()).isZero();
  }

  @Test
  @DisplayName("a required cardinality failure retries the event instead of acknowledging it")
  void requiredCardinalityFailureIsNotSettled() {
    build(1, 1, 0, 100, 3);
    counter.required = true;
    counter.failure = new IllegalStateException("transaction cancelled");

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.delayedRetries() == 1);
    assertThat(ack.acks()).isZero();
    assertThat(registry.get("skier.cardinality.projection.unresolved").counter().count())
        .isEqualTo(1.0);
  }

  @Test
  @DisplayName("a required projection that keeps failing is quarantined with the ride stored")
  void requiredCardinalityFailureIsEventuallyQuarantined() {
    build(1, 1, 0, 100, 1);
    counter.required = true;
    counter.failure = new IllegalStateException("transaction cancelled");

    int attempts = 0;
    for (int delivery = 1; delivery <= 2; delivery++) {
      RecordingAck ack = new RecordingAck();
      processor.submit(event(42, 100, ack, attempts));
      if (delivery == 1) {
        await().atMost(Duration.ofSeconds(5)).until(() -> ack.delayedRetries() == 1);
        attempts = ack.lastAttempt();
      } else {
        await().atMost(Duration.ofSeconds(5)).until(() -> ack.deadLetters() == 1);
      }
    }
  }

  @Test
  @DisplayName(
      "a full staging queue returns the event to the broker instead of blocking or dropping")
  void appliesBackpressureWhenSaturated() {
    build(1, 1, 0, 1, 3);
    writer.blockWrites();

    List<RecordingAck> acks = new ArrayList<>();
    int rejected = 0;
    for (int i = 0; i < 200; i++) {
      RecordingAck ack = new RecordingAck();
      acks.add(ack);
      if (!processor.submit(event(i, 100 + i, ack))) {
        rejected++;
      }
    }

    assertThat(rejected).isPositive();
    assertThat(acks.stream().mapToInt(RecordingAck::requeues).sum())
        .as("every refused event must be handed back to the broker")
        .isEqualTo(rejected);

    writer.release();
  }

  @Test
  @DisplayName("a whole-request failure sends every event in the group to the delay route")
  void retriesOnWriteFailure() {
    build(1, 1, 0, 100, 3);
    writer.failEveryWriteWith(new IllegalStateException("throttled"));

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.delayedRetries() == 1);
    assertThat(ack.acks()).isZero();
    assertThat(ack.deadLetters()).isZero();
    assertThat(ack.lastAttempt()).isEqualTo(1);
  }

  @Test
  @DisplayName("a delay route that will not take the retry requeues instead of acking")
  void requeuesWhenTheDelayRouteRefuses() {
    build(1, 1, 0, 100, 3);
    writer.failEveryWriteWith(new IllegalStateException("throttled"));

    RecordingAck ack = new RecordingAck();
    ack.breakRetryRoute();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.requeues() == 1);
    assertThat(ack.acks()).isZero();
    assertThat(registry.get("skier.write.retry.handoff.failed").counter().count()).isEqualTo(1.0);
  }

  @Test
  @DisplayName("an event that exhausts its retry budget is dead-lettered, not retried forever")
  void deadLettersAfterRetryBudget() {
    build(1, 1, 0, 100, 2);
    writer.failEveryWriteWith(new IllegalStateException("permanently broken"));

    // Each delivery is a new event seeded from the count the previous one handed back.
    int attempts = 0;
    for (int delivery = 1; delivery <= 3; delivery++) {
      RecordingAck ack = new RecordingAck();
      processor.submit(event(42, 100, ack, attempts));

      if (delivery <= 2) {
        int expected = delivery;
        await().atMost(Duration.ofSeconds(5)).until(() -> ack.delayedRetries() == 1);
        assertThat(ack.lastAttempt()).isEqualTo(expected);
        attempts = ack.lastAttempt();
      } else {
        await().atMost(Duration.ofSeconds(5)).until(() -> ack.deadLetters() == 1);
        assertThat(ack.delayedRetries()).isZero();
      }
    }
  }

  @Test
  @DisplayName("a delivery that arrives with a spent budget is dead-lettered on its first attempt")
  void honoursAnAlreadySpentBudgetFromTheWire() {
    build(1, 1, 0, 100, 2);
    writer.failEveryWriteWith(new IllegalStateException("permanently broken"));

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack, 2));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.deadLetters() == 1);
    assertThat(ack.delayedRetries()).isZero();
  }

  @Test
  @DisplayName("unprocessed items are retried while their successful neighbours are acknowledged")
  void settlesPartialBatchIndependently() {
    build(1, 5, 50, 100, 3);
    // BatchWriteItem returns 200 with an UnprocessedItems map when individual items throttle.
    writer.deferItems(events -> List.of(events.get(0)));

    List<RecordingAck> acks = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      RecordingAck ack = new RecordingAck();
      acks.add(ack);
      processor.submit(event(i, 100 + i, ack));
    }

    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> acks.stream().allMatch(RecordingAck::isSettled));

    assertThat(acks.stream().mapToInt(RecordingAck::acks).sum()).isEqualTo(4);
    assertThat(acks.stream().mapToInt(RecordingAck::delayedRetries).sum()).isEqualTo(1);
    assertThat(acks.stream().mapToInt(RecordingAck::requeues).sum()).isZero();
  }

  @Test
  @DisplayName("events are coalesced into groups up to the configured batch size")
  void coalescesIntoBatches() {
    build(1, 10, 200, 1000, 3);
    CountDownLatch gate = writer.blockWrites();

    List<RecordingAck> acks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RecordingAck ack = new RecordingAck();
      acks.add(ack);
      processor.submit(event(i, 100 + i, ack));
    }

    gate.countDown();
    writer.release();

    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> acks.stream().mapToInt(RecordingAck::acks).sum() == 10);

    assertThat(writer.writeCallCount()).isLessThan(10);
    assertThat(writer.itemsSeen()).isEqualTo(10);
  }

  @Test
  @DisplayName("the linger window bounds how long a partial group waits")
  void flushesPartialBatchAfterLinger() {
    build(1, 25, 30, 1000, 3);

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(2)).until(() -> ack.acks() == 1);
    assertThat(writer.batches().get(0)).hasSize(1);
  }

  @Test
  @DisplayName("shutdown drains staged events and returns anything unfinished to the broker")
  void shutdownReturnsUnfinishedWork() {
    build(1, 1, 0, 1000, 3);
    writer.blockWrites();

    List<RecordingAck> acks = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      RecordingAck ack = new RecordingAck();
      acks.add(ack);
      processor.submit(event(i, 100 + i, ack));
    }

    writer.release();
    processor.shutdown();
    processor = null;

    assertThat(acks).allSatisfy(ack -> assertThat(ack.isSettled()).isTrue());
    assertThat(acks.stream().mapToInt(a -> a.acks() + a.requeues() + a.deadLetters()).sum())
        .isEqualTo(20);
  }

  @Test
  @DisplayName("submitting after shutdown returns the event rather than accepting it")
  void refusesSubmissionsAfterShutdown() {
    build(1, 1, 0, 100, 3);
    processor.shutdown();

    RecordingAck ack = new RecordingAck();
    assertThat(processor.submit(event(42, 100, ack))).isFalse();
    assertThat(ack.requeues()).isEqualTo(1);

    processor = null;
  }

  @Test
  @DisplayName("a durable event contributes one freshness sample measured from its publish time")
  void recordsFreshnessOnDurableWrite() {
    build(1, 1, 0, 100, 3);
    long publishedAt = System.currentTimeMillis() - 400;

    RecordingAck ack = new RecordingAck();
    processor.submit(stampedEvent(42, publishedAt, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.acks() == 1);

    Timer freshness = registry.get("skier.pipeline.freshness").timer();
    assertThat(freshness.count()).isEqualTo(1);
    // The 400 ms predates the pipeline, so write latency cannot see it and freshness must.
    assertThat(freshness.max(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(400.0);
    assertThat(registry.get("skier.write.latency").timer().max(TimeUnit.MILLISECONDS))
        .isLessThan(400.0);
  }

  @Test
  @DisplayName("a dead-lettered event produces no freshness sample")
  void doesNotRecordFreshnessForAnEventThatNeverBecameDurable() {
    build(1, 1, 0, 100, 0);
    writer.failEveryWriteWith(new IllegalStateException("permanently broken"));

    RecordingAck ack = new RecordingAck();
    processor.submit(stampedEvent(42, System.currentTimeMillis() - 400, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.deadLetters() == 1);

    assertThat(registry.get("skier.pipeline.freshness").timer().count()).isZero();
    assertThat(registry.get("skier.pipeline.freshness.unstamped").counter().count()).isZero();
  }

  @Test
  @DisplayName("an unstamped durable event is counted as unmeasured rather than ignored")
  void countsDurableEventsThatCarryNoPublishTimestamp() {
    build(1, 1, 0, 100, 3);

    RecordingAck ack = new RecordingAck();
    processor.submit(event(42, 100, ack));

    await().atMost(Duration.ofSeconds(5)).until(() -> ack.acks() == 1);

    assertThat(registry.get("skier.pipeline.freshness").timer().count()).isZero();
    assertThat(registry.get("skier.pipeline.freshness.unstamped").counter().count()).isEqualTo(1.0);
  }

  @Test
  @DisplayName("concurrent writers settle every event exactly once")
  void settlesEveryEventExactlyOnceUnderConcurrency() {
    build(8, 5, 10, 5000, 3);

    List<RecordingAck> acks = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      RecordingAck ack = new RecordingAck();
      acks.add(ack);
      processor.submit(event(i, 100 + (i % 250), ack));
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> acks.stream().allMatch(RecordingAck::isSettled));

    assertThat(acks.stream().mapToInt(RecordingAck::acks).sum()).isEqualTo(500);
    assertThat(writer.itemsSeen()).isEqualTo(500);
  }
}
