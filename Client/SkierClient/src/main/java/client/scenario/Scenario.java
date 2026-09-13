package client.scenario;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** A load run, described as data. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Scenario {

  private String name = "unnamed";
  private String description = "";
  private Workload workload = new Workload();
  private Http http = new Http();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Workload getWorkload() {
    return workload;
  }

  public void setWorkload(Workload workload) {
    this.workload = workload;
  }

  public Http getHttp() {
    return http;
  }

  public void setHttp(Http http) {
    this.http = http;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Workload {
    private int skiers = 100_000;
    private int resortId = 5;
    private int seasonId = 2025;
    private int dayId = 1;
    private int lifts = 40;
    private int minutesInSkiDay = 360;

    private long seed = 20250421L;

    public int getSkiers() {
      return skiers;
    }

    public void setSkiers(int skiers) {
      this.skiers = skiers;
    }

    public int getResortId() {
      return resortId;
    }

    public void setResortId(int resortId) {
      this.resortId = resortId;
    }

    public int getSeasonId() {
      return seasonId;
    }

    public void setSeasonId(int seasonId) {
      this.seasonId = seasonId;
    }

    public int getDayId() {
      return dayId;
    }

    public void setDayId(int dayId) {
      this.dayId = dayId;
    }

    public int getLifts() {
      return lifts;
    }

    public void setLifts(int lifts) {
      this.lifts = lifts;
    }

    public int getMinutesInSkiDay() {
      return minutesInSkiDay;
    }

    public void setMinutesInSkiDay(int minutesInSkiDay) {
      this.minutesInSkiDay = minutesInSkiDay;
    }

    public long getSeed() {
      return seed;
    }

    public void setSeed(long seed) {
      this.seed = seed;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Http {
    private int maxConnections = 1000;
    private int maxConnectionsPerRoute = 500;
    private int connectTimeoutMs = 5000;
    private int socketTimeoutMs = 10_000;
    private int ioThreads = Runtime.getRuntime().availableProcessors();

    private int maxRetries = 3;

    private long retryBaseBackoffMs = 50;

    public int getMaxConnections() {
      return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
    }

    public int getMaxConnectionsPerRoute() {
      return maxConnectionsPerRoute;
    }

    public void setMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
      this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    }

    public int getConnectTimeoutMs() {
      return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
      this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
      return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
      this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getIoThreads() {
      return ioThreads;
    }

    public void setIoThreads(int ioThreads) {
      this.ioThreads = ioThreads;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    public long getRetryBaseBackoffMs() {
      return retryBaseBackoffMs;
    }

    public void setRetryBaseBackoffMs(long retryBaseBackoffMs) {
      this.retryBaseBackoffMs = retryBaseBackoffMs;
    }
  }
}
