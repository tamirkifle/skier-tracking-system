package skiers.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import skiers.service.FleetRegistry;
import skiers.service.RedisFleetRegistry;

/** Selects the fleet registry; a missing-bean fallback would be order-dependent. */
@Configuration
public class FleetConfig {

  @Bean
  @ConditionalOnProperty(
      value = "skier.fleet.coordination-enabled",
      havingValue = "true",
      matchIfMissing = true)
  public FleetRegistry redisFleetRegistry(
      StringRedisTemplate redis, SkierProperties properties, MeterRegistry registry) {
    return new RedisFleetRegistry(redis, properties, registry);
  }

  @Bean
  @ConditionalOnProperty(value = "skier.fleet.coordination-enabled", havingValue = "false")
  public FleetRegistry singleInstanceFleetRegistry() {
    return FleetRegistry.singleInstance();
  }
}
