package skiers.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "skier")
public class SkierProperties {

  @NestedConfigurationProperty private final Admission admission = new Admission();

  @NestedConfigurationProperty private final Ingest ingest = new Ingest();

  @NestedConfigurationProperty private final QueueMonitor queueMonitor = new QueueMonitor();

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

  public QueueMonitor getQueueMonitor() {
    return queueMonitor;
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

  public enum SetpointMode {
    DEPTH,

    LATENCY
  }

  public static class QueueMonitor {
    private boolean enabled = true;

    private long intervalMs = 200;

    private SetpointMode setpointMode = SetpointMode.DEPTH;

    /** The 150-message setpoint divided by a drain rate of 80 events/s. */
    private Duration targetLatency = Duration.ofMillis(1875);

    private Duration maxLatency = Duration.ofMillis(2500);

    private Duration minLatency = Duration.ofMillis(1250);

    private Duration drainRateWindow = Duration.ofMillis(10000);

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }

    public SetpointMode getSetpointMode() {
      return setpointMode;
    }

    public void setSetpointMode(SetpointMode setpointMode) {
      this.setpointMode = setpointMode;
    }

    public Duration getTargetLatency() {
      return targetLatency;
    }

    public void setTargetLatency(Duration targetLatency) {
      this.targetLatency = targetLatency;
    }

    public Duration getMaxLatency() {
      return maxLatency;
    }

    public void setMaxLatency(Duration maxLatency) {
      this.maxLatency = maxLatency;
    }

    public Duration getMinLatency() {
      return minLatency;
    }

    public void setMinLatency(Duration minLatency) {
      this.minLatency = minLatency;
    }

    public Duration getDrainRateWindow() {
      return drainRateWindow;
    }

    public void setDrainRateWindow(Duration drainRateWindow) {
      this.drainRateWindow = drainRateWindow;
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
