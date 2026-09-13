package skiers.health;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import skiers.metrics.IngestMetrics;
import skiers.service.QueueMonitor;
import skiers.service.RateLimiter;

/** Admission-control state. Shedding is UP; admission pinned at the floor is SATURATED. */
@Component("pipeline")
public class PipelineHealthIndicator implements HealthIndicator {

  private final RateLimiter rateLimiter;
  private final IngestMetrics metrics;
  private final ObjectProvider<QueueMonitor> queueMonitor;

  public PipelineHealthIndicator(
      RateLimiter rateLimiter, IngestMetrics metrics, ObjectProvider<QueueMonitor> queueMonitor) {
    this.rateLimiter = rateLimiter;
    this.metrics = metrics;
    this.queueMonitor = queueMonitor;
  }

  @Override
  public Health health() {
    int rate = rateLimiter.getCurrentRate();
    int floor = rateLimiter.effectiveFloor();
    boolean saturated = rate <= floor;

    Health.Builder builder = saturated ? Health.status("SATURATED") : Health.up();
    builder
        .withDetail("admissionRatePerSecond", rate)
        .withDetail("admissionRateFloorPerSecond", floor)
        .withDetail("admissionRateFloorConfigured", rateLimiter.configuredFloor())
        .withDetail("availablePermits", rateLimiter.availableTokens())
        .withDetail("accepted", (long) metrics.acceptedCount())
        .withDetail("shed", (long) metrics.shedCount());

    QueueMonitor monitor = queueMonitor.getIfAvailable();
    if (monitor != null) {
      int depth = monitor.lastObservedDepth();
      builder.withDetail("queueDepth", depth >= 0 ? depth : "unknown");
    }

    return builder.build();
  }
}
