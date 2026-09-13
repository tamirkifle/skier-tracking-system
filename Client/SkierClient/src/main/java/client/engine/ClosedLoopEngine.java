package client.engine;

import client.generator.EventFactory;
import client.metrics.LatencyRecorder;
import client.scenario.Scenario;
import client.transport.HttpTransport;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClosedLoopEngine implements LoadEngine {

  private static final Logger log = LoggerFactory.getLogger(ClosedLoopEngine.class);

  private static final int OUTSTANDING_PER_WORKER = 1;

  @Override
  public Dispatch run(
      Scenario.Phase phase,
      EventFactory events,
      HttpTransport transport,
      LatencyRecorder recorder,
      ProgressSink progress)
      throws InterruptedException {

    long startNanos = System.nanoTime();
    int threads = phase.getThreads();
    int perThread = phase.getRequestsPerThread();
    AtomicLong issued = new AtomicLong();
    CountDownLatch done = new CountDownLatch(threads);

    log.info(
        "Phase '{}': closed loop, {} threads x {} requests = {}",
        phase.getName(),
        threads,
        perThread,
        (long) threads * perThread);

    ExecutorService pool =
        Executors.newFixedThreadPool(
            threads,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("load-worker-" + thread.getId());
              thread.setDaemon(true);
              return thread;
            });

    ConcurrencySampler sampler = ConcurrencySampler.start(transport, progress);

    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(
            () -> {
              try {
                Semaphore outstanding = new Semaphore(OUTSTANDING_PER_WORKER);
                for (int i = 0; i < perThread; i++) {
                  outstanding.acquire();
                  long sentAt = System.nanoTime();
                  transport
                      .send(events.next(), sentAt)
                      .whenComplete((status, error) -> outstanding.release());
                  issued.incrementAndGet();
                }
                outstanding.acquire(OUTSTANDING_PER_WORKER);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }

      done.await();
    } finally {
      sampler.stop();
      pool.shutdownNow();
    }

    return Dispatch.onDemand(issued.get(), (System.nanoTime() - startNanos) / 1_000_000_000d);
  }

  static final class ConcurrencySampler {
    private final java.util.concurrent.ScheduledExecutorService scheduler;

    private ConcurrencySampler(java.util.concurrent.ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
    }

    static ConcurrencySampler start(HttpTransport transport, ProgressSink progress) {
      var scheduler =
          Executors.newSingleThreadScheduledExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "concurrency-sampler");
                thread.setDaemon(true);
                return thread;
              });
      scheduler.scheduleAtFixedRate(
          () -> progress.sampleConcurrency(transport.inFlight()),
          0,
          100,
          java.util.concurrent.TimeUnit.MILLISECONDS);
      return new ConcurrencySampler(scheduler);
    }

    void stop() {
      scheduler.shutdownNow();
    }
  }
}
