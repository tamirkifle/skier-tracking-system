package client;

import client.clientpart1.EventGenerator;
import client.clientpart1.LiftEventWorker;
import client.clientpart2.LatencyRecorder;
import client.clientpart2.LatencyRecorder.LatencyRecord;
import client.clientpart2.MetricsCalculator;
import client.common.HttpClientManager;
import client.common.model.LiftRideEvent;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkierClient {
  private static final Logger log = LoggerFactory.getLogger(SkierClient.class);
  private static final String BASE_URL = "http://SkierServerLB-817885713.us-west-2.elb.amazonaws.com:8080/skiers";

  //private static final String BASE_URL = "http://35.161.216.133:8080/skiers";


  //http://SkierServerLB-817885713.us-west-2.elb.amazonaws.com:8080/skiers/5/seasons/2025/days/1/skier/65148
  // Phase 1
  private static final int PHASE1_TOTAL_REQUESTS = 32000;
  private static final int PHASE1_THREADS = 200; // changed thread count for Assignment 4
  private static final int PHASE1_REQUESTS_PER_THREAD = 160;

  // Phase 2
  private static final int PHASE2_TOTAL_REQUESTS = 168000;
  private static final int PHASE2_THREADS = 200;
  private static final int PHASE2_REQUESTS_PER_THREAD = 840;

  private static final AtomicInteger activeRequests = new AtomicInteger(0);
  private static final List<Integer> phase1ActiveRequestsOverTime = new ArrayList<>();
  private static final List<Integer> phase2ActiveRequestsOverTime = new ArrayList<>();

  public static void main(String[] args) throws InterruptedException {

    /*===== Client Configuration =====
    Initial Threads: 32
    Requests per Initial Thread: 1000
    Total Requests: 200000*/


    // Producer
    EventGenerator generator = new EventGenerator(200_000, BASE_URL);
    Thread generatorThread = new Thread(generator);
    generatorThread.start();
    BlockingQueue<LiftRideEvent> eventQueue = generator.getQueue();

    // Pre-processing
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    LatencyRecorder latencyRecorder = new LatencyRecorder();
    HttpClientManager clientManager = new HttpClientManager(latencyRecorder, activeRequests);

    log.info("\n=== Client Configuration === ");
    log.info("Initial Threads: {}", PHASE1_THREADS);
    log.info("Requests per Initial Thread: {}", PHASE1_REQUESTS_PER_THREAD);
    log.info("Total Requests: {}", PHASE1_TOTAL_REQUESTS + PHASE2_TOTAL_REQUESTS);

    // Phase 1 execution
    log.info("\n=== Phase 1 ===");
    log.info("Submitting {} threads. Each processing {} requests.", PHASE1_THREADS, PHASE1_REQUESTS_PER_THREAD);
    CountDownLatch phase1Latch = new CountDownLatch(PHASE1_THREADS);
    CountDownLatch phase1RequestsLatch = new CountDownLatch(PHASE1_THREADS * PHASE1_REQUESTS_PER_THREAD);

    // Start tracking active requests for Phase 1
    scheduler.scheduleAtFixedRate(() -> {
      phase1ActiveRequestsOverTime.add(activeRequests.get());
    }, 0, 100, TimeUnit.MILLISECONDS);

    long phase1StartTime = System.currentTimeMillis();
    List<Future<?>> phase1Futures = new ArrayList<>();
    for (int i = 0; i < PHASE1_THREADS; i++) {
      phase1Futures.add(executorService.submit(new LiftEventWorker(eventQueue, PHASE1_REQUESTS_PER_THREAD, phase1Latch, clientManager, phase1RequestsLatch, generator)));
    }

    // Wait for Phase 1 to complete
    for (Future<?> future : phase1Futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        log.error("Error in Phase 1 execution: ", e);
      }
    }
    phase1Latch.await();
    phase1RequestsLatch.await();
    long phase1EndTime = System.currentTimeMillis();
    long phase1WallTime = phase1EndTime - phase1StartTime;

    // Calculate Phase 1 throughput
    double phase1Throughput = (double) PHASE1_TOTAL_REQUESTS / (phase1WallTime / 1000.0);
    log.info("Phase 1 Throughput: {} requests/second", phase1Throughput);

    // Calculate Phase 1 average active requests
    double phase1AverageActiveRequests = phase1ActiveRequestsOverTime.stream()
        .mapToInt(Integer::intValue)
        .average()
        .orElse(0.0);
    //log.info("Phase 1 Average Active Requests: {}", phase1AverageActiveRequests);

    // Calculate Phase 1 throughput using Little's Law
    long phase1TotalLatency = 0;
    synchronized (latencyRecorder.getLatencies()) {
      for (LatencyRecord latency : latencyRecorder.getLatencies()) {
        phase1TotalLatency += latency.getLatency();
      }
    }
    double phase1AvgLatency = (double) phase1TotalLatency / latencyRecorder.getLatencies().size(); // Average latency in milliseconds
    double phase1AvgLatencySeconds = phase1AvgLatency / 1000.0; // Convert to seconds
    double phase1LittleThroughput = phase1AverageActiveRequests / phase1AvgLatencySeconds;
    log.info("Phase 1 Throughput calculated through latency: {} requests/second", phase1LittleThroughput);

    // Reset active requests for Phase 2
    activeRequests.set(0);

    // Phase 2 execution
    log.info("\n=== Phase 2 ===");
    log.info("Submitting {} threads. Each processing {} requests.", PHASE2_THREADS, PHASE2_REQUESTS_PER_THREAD);
    CountDownLatch phase2Latch = new CountDownLatch(PHASE2_THREADS);
    CountDownLatch phase2RequestsLatch = new CountDownLatch(PHASE2_THREADS * PHASE2_REQUESTS_PER_THREAD);

    // Start tracking active requests for Phase 2
    scheduler.scheduleAtFixedRate(() -> {
      phase2ActiveRequestsOverTime.add(activeRequests.get());
    }, 0, 100, TimeUnit.MILLISECONDS);

    long phase2StartTime = System.currentTimeMillis();
    List<Future<?>> phase2Futures = new ArrayList<>();
    for (int i = 0; i < PHASE2_THREADS; i++) {
      phase2Futures.add(executorService.submit(new LiftEventWorker(eventQueue, PHASE2_REQUESTS_PER_THREAD, phase2Latch, clientManager, phase2RequestsLatch, generator)));
    }

    // Wait for Phase 2 to complete
    for (Future<?> future : phase2Futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        log.error("Error in Phase 2 execution: ", e);
      }
    }
    phase2Latch.await();
    phase2RequestsLatch.await();
    long phase2EndTime = System.currentTimeMillis();
    long phase2WallTime = phase2EndTime - phase2StartTime;

    // Calculate Phase 2 throughput
    double phase2Throughput = (double) PHASE2_TOTAL_REQUESTS / (phase2WallTime / 1000.0);
    log.info("Phase 2 Throughput: {} requests/second", phase2Throughput);

    // Calculate Phase 2 average active requests
    double phase2AverageActiveRequests = phase2ActiveRequestsOverTime.stream()
        .mapToInt(Integer::intValue)
        .average()
        .orElse(0.0);
    // log.info("Phase 2 Average Active Requests: {}", phase2AverageActiveRequests);

    // Calculate Phase 2 throughput using Little's Law
    long phase2TotalLatency = 0;
    synchronized (latencyRecorder.getLatencies()) {
      for (LatencyRecord latency : latencyRecorder.getLatencies()) {
        phase2TotalLatency += latency.getLatency();
      }
    }
    double phase2AvgLatency = (double) phase2TotalLatency / latencyRecorder.getLatencies().size(); // Average latency in milliseconds
    double phase2AvgLatencySeconds = phase2AvgLatency / 1000.0; // Convert to seconds
    double phase2LittleThroughput = phase2AverageActiveRequests / phase2AvgLatencySeconds;
    log.info("Phase 2 Throughput calculated through latency: {} requests/second", phase2LittleThroughput);

    // Overall calculations
    long totalWallTime = phase2EndTime - phase1StartTime;
    double combinedThroughput = (double) (PHASE1_TOTAL_REQUESTS + PHASE2_TOTAL_REQUESTS) / (totalWallTime / 1000.0);
    log.info("\n=== Overall Results ===");
    log.info("Total Throughput (Phase 1 & 2 Combined): {} requests/second", combinedThroughput);

    // Calculate overall average active requests
    List<Integer> combinedActiveRequestsOverTime = new ArrayList<>();
    combinedActiveRequestsOverTime.addAll(phase1ActiveRequestsOverTime);
    combinedActiveRequestsOverTime.addAll(phase2ActiveRequestsOverTime);
    double combinedAverageActiveRequests = combinedActiveRequestsOverTime.stream()
        .mapToInt(Integer::intValue)
        .average()
        .orElse(0.0);
    //log.info("Combined Average Active Requests: {}", combinedAverageActiveRequests);

    // Calculate overall throughput calculated through latency
    long combinedTotalLatency = phase1TotalLatency + phase2TotalLatency;
    double combinedAvgLatency = (double) combinedTotalLatency / latencyRecorder.getLatencies().size(); // Average latency in milliseconds
    double combinedAvgLatencySeconds = combinedAvgLatency / 1000.0; // Convert to seconds
    double combinedLittleThroughput = combinedAverageActiveRequests / combinedAvgLatencySeconds;
    log.info("Combined Throughput calculated through latency: {} requests/second", combinedLittleThroughput);

    log.info("\n=== Final Results ===");
    log.info("Total successful requests: {}", latencyRecorder.getSuccessfulRequests());
    log.info("Total failed requests: {}", latencyRecorder.getFailedRequests());
    log.info("Total wall time: {} ms", totalWallTime);
    log.info("Throughput {} requests/second", combinedThroughput);
    log.info("Throughput calculted through latency: {} requests/second", combinedLittleThroughput);

    // Shutdown
    scheduler.shutdown();
    executorService.shutdown();
    clientManager.shutdown();

    // Print final metrics
    MetricsCalculator.calculateAndPrintMetrics(latencyRecorder.getLatencies());
  }
}