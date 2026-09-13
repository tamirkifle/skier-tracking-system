package skiers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
