package skiers.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "skier")
public class SkierProperties {

  @NestedConfigurationProperty private final Admission admission = new Admission();

  @NestedConfigurationProperty private final Ingest ingest = new Ingest();

  @NestedConfigurationProperty private final Cache cache = new Cache();

  public Admission getAdmission() {
    return admission;
  }

  public Ingest getIngest() {
    return ingest;
  }

  public static class Ingest {

    private long confirmTimeoutMs = 5000;

    private int maxEventIdLength = 64;

    public long getConfirmTimeoutMs() {
      return confirmTimeoutMs;
    }

    public void setConfirmTimeoutMs(long confirmTimeoutMs) {
      this.confirmTimeoutMs = confirmTimeoutMs;
    }

    public int getMaxEventIdLength() {
      return maxEventIdLength;
    }

    public void setMaxEventIdLength(int maxEventIdLength) {
      this.maxEventIdLength = maxEventIdLength;
    }
  }

  public Cache getCache() {
    return cache;
  }

  public static class Admission {
    private int initialCapacity = 1700;

    /** A floor above what the pipeline can drain cannot be corrected by the control law. */
    private int minRate = 100;

    private long refillIntervalMs = 10;

    /** Unbounded waiting accumulates load rather than shedding it, filling the thread pool. */
    private long maxWaitMs = 250;

    private long waitSliceMs = 5;

    public int getInitialCapacity() {
      return initialCapacity;
    }

    public void setInitialCapacity(int initialCapacity) {
      this.initialCapacity = initialCapacity;
    }

    public int getMinRate() {
      return minRate;
    }

    public void setMinRate(int minRate) {
      this.minRate = minRate;
    }

    public long getRefillIntervalMs() {
      return refillIntervalMs;
    }

    public void setRefillIntervalMs(long refillIntervalMs) {
      this.refillIntervalMs = refillIntervalMs;
    }

    public long getMaxWaitMs() {
      return maxWaitMs;
    }

    public void setMaxWaitMs(long maxWaitMs) {
      this.maxWaitMs = maxWaitMs;
    }

    public long getWaitSliceMs() {
      return waitSliceMs;
    }

    public void setWaitSliceMs(long waitSliceMs) {
      this.waitSliceMs = waitSliceMs;
    }
  }

  public static class Cache {
    private Duration ttl = Duration.ofHours(1);

    public Duration getTtl() {
      return ttl;
    }

    public void setTtl(Duration ttl) {
      this.ttl = ttl;
    }
  }
}
