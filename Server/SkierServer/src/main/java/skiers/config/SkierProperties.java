package skiers.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "skier")
public class SkierProperties {

  @NestedConfigurationProperty private final Ingest ingest = new Ingest();

  @NestedConfigurationProperty private final Cache cache = new Cache();

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
