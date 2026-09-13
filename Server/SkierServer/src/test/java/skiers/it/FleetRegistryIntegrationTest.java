package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import skiers.config.SkierProperties;
import skiers.service.RedisFleetRegistry;

@Tag("integration")
@Testcontainers
class FleetRegistryIntegrationTest {

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

  static final GenericContainer<?> REDIS =
      new GenericContainer<>(REDIS_IMAGE)
          .withExposedPorts(6379)
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
          .withReuse(true);

  private static StringRedisTemplate template;

  static {
    REDIS.start();
    LettuceConnectionFactory factory =
        new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
    factory.afterPropertiesSet();
    template = new StringRedisTemplate(factory);
  }

  private static SkierProperties propertiesForThisRun() {
    SkierProperties properties = new SkierProperties();
    properties.getFleet().setKey("skier:test:fleet:" + UUID.randomUUID());
    properties.getFleet().setHeartbeatIntervalMs(100);
    properties.getFleet().setMemberTtlMs(300);
    return properties;
  }

  private static RedisFleetRegistry registryOn(SkierProperties properties) {
    return new RedisFleetRegistry(template, properties, new SimpleMeterRegistry());
  }

  @Test
  @DisplayName("independent instances sharing a key count each other")
  void instancesCountEachOther() throws InterruptedException {
    SkierProperties shared = propertiesForThisRun();
    RedisFleetRegistry a = registryOn(shared);
    RedisFleetRegistry b = registryOn(shared);

    a.refresh();
    b.refresh();
    a.refresh();

    assertThat(a.size()).as("a, after b has registered").isEqualTo(2);
    assertThat(b.size()).as("b, which saw a already").isEqualTo(2);

    RedisFleetRegistry c = registryOn(shared);
    c.refresh();
    a.refresh();
    assertThat(a.size()).as("a, after a third replica joins").isEqualTo(3);
    assertThat(c.size()).isEqualTo(3);
  }

  @Test
  @DisplayName("an instance that stops heartbeating is dropped, and the survivor notices")
  void aSilentInstanceExpires() throws InterruptedException {
    SkierProperties shared = propertiesForThisRun();
    RedisFleetRegistry alive = registryOn(shared);
    RedisFleetRegistry doomed = registryOn(shared);

    alive.refresh();
    doomed.refresh();
    alive.refresh();
    assertThat(alive.size()).isEqualTo(2);

    // Past the 300ms member TTL, so the survivor's next refresh prunes doomed from the set.
    Thread.sleep(400);
    alive.refresh();
    assertThat(alive.size())
        .as("first refresh after the prune: shrink not yet confirmed")
        .isEqualTo(2);
    alive.refresh();
    assertThat(alive.size()).as("second refresh: still not confirmed").isEqualTo(2);
    alive.refresh();

    assertThat(alive.size()).as("third consecutive refresh confirms the shrink").isEqualTo(1);
  }
}
