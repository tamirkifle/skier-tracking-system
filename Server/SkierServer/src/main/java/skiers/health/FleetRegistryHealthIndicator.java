package skiers.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import skiers.service.FleetRegistry;

@Component("fleetRegistry")
public class FleetRegistryHealthIndicator implements HealthIndicator {

  private final FleetRegistry fleet;

  public FleetRegistryHealthIndicator(FleetRegistry fleet) {
    this.fleet = fleet;
  }

  @Override
  public Health health() {
    boolean sized = fleet.isSized();
    Health.Builder builder =
        sized
            ? Health.up()
            // OUT_OF_SERVICE rather than DOWN: this instance is not in service yet.
            : Health.outOfService()
                .withDetail(
                    "reason",
                    "the fleet size has never been observed, so this instance cannot take its"
                        + " share of the admission floor");
    return builder.withDetail("sized", sized).withDetail("size", fleet.size()).build();
  }
}
