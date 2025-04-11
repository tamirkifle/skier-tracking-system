package client.common;

import client.clientpart2.LatencyRecorder;
import client.common.model.LiftRideEvent;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HttpClientManager {
  private static final Logger logger = Logger.getLogger(HttpClientManager.class.getName());

  private final CloseableHttpAsyncClient httpAsyncClient;
  private final Gson gson;
  private final LatencyRecorder latencyRecorder;
  private final AtomicInteger activeRequests;

  public HttpClientManager(LatencyRecorder latencyRecorder, AtomicInteger activeRequests) {
    this.latencyRecorder = latencyRecorder;
    this.activeRequests = activeRequests;

    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setIoThreadCount(Runtime.getRuntime().availableProcessors())
        .setSoTimeout(5000)
        .setConnectTimeout(5000)
        .build();

    ConnectingIOReactor ioReactor;
    try {
      ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    } catch (IOReactorException e) {
      throw new RuntimeException("Failed to create IO reactor", e);
    }

    PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
    cm.setMaxTotal(1000);
    cm.setDefaultMaxPerRoute(500);

    this.httpAsyncClient = HttpAsyncClients.custom()
        .setConnectionManager(cm)
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(5000)
            .setSocketTimeout(5000)
            .build())
        .build();

    this.httpAsyncClient.start();
    this.gson = new Gson();
  }

  public void shutdown() {
    try {
      httpAsyncClient.close();
    } catch (IOException e) {
      logger.log(Level.WARNING, "Error closing async HTTP client", e);
    }
  }

  private static class LiftRideBody {
    private final int time;
    private final int liftID;

    public LiftRideBody(int time, int liftID) {
      this.time = time;
      this.liftID = liftID;
    }
  }

  public CompletableFuture<HttpResponse> sendPostRequest(LiftRideEvent event) {
    CompletableFuture<HttpResponse> resultFuture = new CompletableFuture<>();
    long startTime = System.nanoTime();  // Capture initial request start time
    executeWithRetry(event, resultFuture, 5, startTime);  // Start with 5 retries
    return resultFuture;
  }

  private void executeWithRetry(LiftRideEvent event, CompletableFuture<HttpResponse> future,
      int retriesLeft, long startTime) {
    activeRequests.incrementAndGet();
    HttpPost post = new HttpPost(event.getFormattedUrl());
    //System.out.println("POST: " + event.getFormattedUrl());
    post.setHeader("Content-Type", "application/json");

    try {
      String json = gson.toJson(new LiftRideBody(event.getTime(), event.getLiftID()));
      post.setEntity(new StringEntity(json));

      httpAsyncClient.execute(post, new FutureCallback<HttpResponse>() {
        @Override
        public void completed(HttpResponse response) {
          int statusCode = response.getStatusLine().getStatusCode();
          if ((statusCode >= 400) && (retriesLeft > 0)) {
            // Retry with remaining attempts
            executeWithRetry(event, future, retriesLeft - 1, startTime);
          } else {
            // Final result (success or permanent failure)
            recordLatency(startTime, statusCode);
            future.complete(response);
          }
          activeRequests.decrementAndGet();
        }

        @Override
        public void failed(Exception ex) {
          if (retriesLeft > 0) {
            // Retry on network errors
            executeWithRetry(event, future, retriesLeft - 1, startTime);
          } else {
            // Final failure after all retries
            recordLatency(startTime, 500);  // 500 = Server Error
            future.completeExceptionally(ex);
          }
          activeRequests.decrementAndGet();
        }

        @Override
        public void cancelled() {
          if (retriesLeft > 0) {
            // Retry on cancellation
            executeWithRetry(event, future, retriesLeft - 1, startTime);
          } else {
            // Final cancellation
            recordLatency(startTime, 499);  // 499 = Client Closed Request
            future.cancel(false);
          }
          activeRequests.decrementAndGet();
        }
      });
    } catch (IOException e) {
      activeRequests.decrementAndGet();
      future.completeExceptionally(e);
    }
  }

  private void recordLatency(long startTime, int statusCode) {
    long endTime = System.nanoTime();
    long latencyMs = (endTime - startTime) / 1_000_000;
    latencyRecorder.recordLatency(startTime, "POST", latencyMs, statusCode);
  }
}