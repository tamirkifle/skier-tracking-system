package client.clientpart1;

import client.common.HttpClientManager;
import client.common.model.LiftRideEvent;
import java.util.concurrent.BlockingQueue;
import org.apache.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiftEventWorker implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(LiftEventWorker.class);
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestsToSend;
  private final CountDownLatch latch;
  private final HttpClientManager clientManager;
  private final CountDownLatch totalRequestLatch;
  private final EventGenerator eventGenerator;  // Added field

  public LiftEventWorker(BlockingQueue<LiftRideEvent> queue,
                         int requestsToSend,
                         CountDownLatch latch,
                         HttpClientManager clientManager,
                         CountDownLatch totalRequestLatch,
                         EventGenerator eventGenerator) {  // Modified constructor
    this.queue = queue;
    this.requestsToSend = requestsToSend;
    this.latch = latch;
    this.clientManager = clientManager;
    this.totalRequestLatch = totalRequestLatch;
    this.eventGenerator = eventGenerator;
  }

  @Override
  public void run() {
    List<CompletableFuture<HttpResponse>> futures = new ArrayList<>();
    int requestsSent = 0;

    try {
      while (requestsSent < requestsToSend && !Thread.currentThread().isInterrupted()) {
        LiftRideEvent event = queue.poll(100, TimeUnit.MILLISECONDS);

        if (event == null) {
          if (eventGenerator.isGenerationComplete()) break;
          continue;
        }

        CompletableFuture<HttpResponse> future = clientManager.sendPostRequest(event)
            .handle((response, ex) -> {
              totalRequestLatch.countDown();

              if (ex != null) {
                log.error("Final error after retries: {}", ex.getMessage());
              } else {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode >= 400) {
                  log.error("Failed with status: {}", statusCode);
                }
              }
              return response;
            });

        futures.add(future);
        requestsSent++;

        // Batch processing to prevent memory issues
        if (futures.size() >= 100) {
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
          futures.clear();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Thread interrupted: {}", e.getMessage());
    } finally {
      if (!futures.isEmpty()) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      }
      latch.countDown();
    }
  }
}