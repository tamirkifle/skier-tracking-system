package client.transport;

import client.common.model.LiftRideEvent;
import client.metrics.LatencyRecorder;
import client.scenario.Scenario;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpTransport implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(HttpTransport.class);

  public static final int STATUS_TRANSPORT_ERROR = 599;

  public static final int STATUS_TIMEOUT = 598;

  public static final int STATUS_CANCELLED = 597;

  private final CloseableHttpAsyncClient client;
  private final LatencyRecorder recorder;
  private final Scenario.Http config;
  private final AtomicInteger inFlight = new AtomicInteger();

  public HttpTransport(Scenario.Http config, LatencyRecorder recorder) {
    this.config = config;
    this.recorder = recorder;

    IOReactorConfig reactorConfig =
        IOReactorConfig.custom()
            .setIoThreadCount(config.getIoThreads())
            .setSoTimeout(config.getSocketTimeoutMs())
            .setConnectTimeout(config.getConnectTimeoutMs())
            .setSoKeepAlive(true)
            .setTcpNoDelay(true)
            .build();

    PoolingNHttpClientConnectionManager connectionManager;
    try {
      connectionManager =
          new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(reactorConfig));
    } catch (IOReactorException e) {
      throw new IllegalStateException("Could not start the IO reactor", e);
    }
    connectionManager.setMaxTotal(config.getMaxConnections());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnectionsPerRoute());

    this.client =
        HttpAsyncClients.custom()
            .setConnectionManager(connectionManager)
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectTimeout(config.getConnectTimeoutMs())
                    .setSocketTimeout(config.getSocketTimeoutMs())
                    .setConnectionRequestTimeout(config.getConnectTimeoutMs())
                    .build())
            .build();
    this.client.start();
  }

  public CompletableFuture<Integer> send(LiftRideEvent event, long dueAtNanos) {
    CompletableFuture<Integer> result = new CompletableFuture<>();
    byte[] body = event.jsonBody().getBytes(StandardCharsets.UTF_8);
    attempt(event, body, result, dueAtNanos, 0);
    return result;
  }

  private void attempt(
      LiftRideEvent event,
      byte[] body,
      CompletableFuture<Integer> result,
      long dueAtNanos,
      int attemptNumber) {

    HttpPost post = new HttpPost(event.getFormattedUrl());
    post.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));

    inFlight.incrementAndGet();
    client.execute(
        post,
        new FutureCallback<>() {
          @Override
          public void completed(HttpResponse response) {
            inFlight.decrementAndGet();
            int status = response.getStatusLine().getStatusCode();
            consume(response);

            if (shouldRetry(status) && attemptNumber < config.getMaxRetries()) {
              retryAfterBackoff(event, body, result, dueAtNanos, attemptNumber);
              return;
            }
            finish(result, dueAtNanos, status);
          }

          @Override
          public void failed(Exception cause) {
            inFlight.decrementAndGet();
            int status =
                cause instanceof java.net.SocketTimeoutException
                    ? STATUS_TIMEOUT
                    : STATUS_TRANSPORT_ERROR;
            if (attemptNumber < config.getMaxRetries()) {
              retryAfterBackoff(event, body, result, dueAtNanos, attemptNumber);
              return;
            }
            finish(result, dueAtNanos, status);
          }

          @Override
          public void cancelled() {
            inFlight.decrementAndGet();
            finish(result, dueAtNanos, STATUS_CANCELLED);
          }
        });
  }

  private static boolean shouldRetry(int status) {
    return status == 429 || status == 503 || (status >= 500 && status < 600);
  }

  private void retryAfterBackoff(
      LiftRideEvent event,
      byte[] body,
      CompletableFuture<Integer> result,
      long dueAtNanos,
      int attemptNumber) {

    recorder.recordRetry();
    long ceiling = config.getRetryBaseBackoffMs() * (1L << attemptNumber);
    long delayMs = ThreadLocalRandom.current().nextLong(1, Math.max(2, ceiling + 1));

    RetryScheduler.schedule(
        () -> attempt(event, body, result, dueAtNanos, attemptNumber + 1), delayMs);
  }

  private void finish(CompletableFuture<Integer> result, long dueAtNanos, int status) {
    long completedAt = System.nanoTime();
    recorder.record((completedAt - dueAtNanos) / 1_000L, status, completedAt);
    result.complete(status);
  }

  private static void consume(HttpResponse response) {
    try {
      if (response.getEntity() != null) {
        org.apache.http.util.EntityUtils.consumeQuietly(response.getEntity());
      }
    } catch (RuntimeException e) {
      log.debug("Failed to drain response entity: {}", e.getMessage());
    }
  }

  public int inFlight() {
    return inFlight.get();
  }

  @Override
  public void close() {
    RetryScheduler.shutdown();
    try {
      client.close();
    } catch (IOException e) {
      log.warn("Error closing HTTP client: {}", e.getMessage());
    }
  }
}
