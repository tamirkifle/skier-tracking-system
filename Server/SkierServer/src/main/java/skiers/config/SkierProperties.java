package skiers.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "skier")
public class SkierProperties {

  @NestedConfigurationProperty private final Ingest ingest = new Ingest();

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
}
