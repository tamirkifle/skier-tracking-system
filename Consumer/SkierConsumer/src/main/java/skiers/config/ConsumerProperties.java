package skiers.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "skier")
public class ConsumerProperties {

  @NestedConfigurationProperty private final Consumer consumer = new Consumer();

  @NestedConfigurationProperty private final Writer writer = new Writer();

  @NestedConfigurationProperty private final Cardinality cardinality = new Cardinality();

  public Consumer getConsumer() {
    return consumer;
  }

  public Writer getWriter() {
    return writer;
  }

  public Cardinality getCardinality() {
    return cardinality;
  }

  public static class Consumer {
    private int concurrency = 32;
    private int maxConcurrency = 64;
    private int prefetch = 250;

    public int getConcurrency() {
      return concurrency;
    }

    public void setConcurrency(int concurrency) {
      this.concurrency = concurrency;
    }

    public int getMaxConcurrency() {
      return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
      this.maxConcurrency = maxConcurrency;
    }

    public int getPrefetch() {
      return prefetch;
    }

    public void setPrefetch(int prefetch) {
      this.prefetch = prefetch;
    }
  }

  public static class Writer {
    /** {@code single} = one PutItem per event; {@code batch} = coalesced BatchWriteItem. */
    private Mode mode = Mode.BATCH;

    private int batchSize = 25;

    private long lingerMs = 20;

    private int threads = 16;
    private int queueCapacity = 50_000;

    private int maxRetries = 5;

    private long retryDelayMs = 500;

    private long retryConfirmTimeoutMs = 2000;

    public Mode getMode() {
      return mode;
    }

    public void setMode(Mode mode) {
      this.mode = mode;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public long getLingerMs() {
      return lingerMs;
    }

    public void setLingerMs(long lingerMs) {
      this.lingerMs = lingerMs;
    }

    public int getThreads() {
      return threads;
    }

    public void setThreads(int threads) {
      this.threads = threads;
    }

    public int getQueueCapacity() {
      return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
      this.queueCapacity = queueCapacity;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    public long getRetryDelayMs() {
      return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
      this.retryDelayMs = retryDelayMs;
    }

    public long getRetryConfirmTimeoutMs() {
      return retryConfirmTimeoutMs;
    }

    public void setRetryConfirmTimeoutMs(long retryConfirmTimeoutMs) {
      this.retryConfirmTimeoutMs = retryConfirmTimeoutMs;
    }

    public enum Mode {
      SINGLE,
      BATCH
    }
  }

  public static class Cardinality {
    private Strategy strategy = Strategy.DYNAMODB;

    public Strategy getStrategy() {
      return strategy;
    }

    public void setStrategy(Strategy strategy) {
      this.strategy = strategy;
    }

    public enum Strategy {
      /** Exact. One conditional PutItem sentinel per first sighting of a skier. */
      DYNAMODB,
      /** Approximate (~0.81% relative error), fixed 12 KiB per resort-day, one Redis op. */
      REDIS_HLL
    }
  }
}
