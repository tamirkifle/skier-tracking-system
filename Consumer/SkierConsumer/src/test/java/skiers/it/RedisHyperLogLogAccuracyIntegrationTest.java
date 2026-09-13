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
import skiers.cardinality.RedisHyperLogLogCounter;
import skiers.ingest.DeliveryAck;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@Tag("integration")
@Testcontainers
class RedisHyperLogLogAccuracyIntegrationTest {

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

  private static final int DISTINCT_SKIERS = 12_000;

  static final GenericContainer<?> REDIS =
      new GenericContainer<>(REDIS_IMAGE)
          .withExposedPorts(6379)
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
          .withReuse(true);

  static {
    REDIS.start();
  }

  // A private resort-day: a warm sketch is already saturated before the first PFADD.
  private static final String RESORT = "acc" + UUID.randomUUID();

  private static final String RESORT_DAY = RESORT + "#2025#1";

  private static final class DiscardingDynamoDb implements DynamoDbClient {
    @Override
    public UpdateItemResponse updateItem(UpdateItemRequest request) {
      return UpdateItemResponse.builder().build();
    }

    @Override
    public String serviceName() {
      return DynamoDbClient.SERVICE_NAME;
    }

    @Override
    public void close() {}
  }

  private static LiftRideEvent event(int skierId) {
    return new LiftRideEvent(
        "evt-" + skierId,
        Integer.toString(skierId),
        RESORT,
        "2025",
        "1",
        21,
        100,
        DeliveryAck.NONE);
  }

  @Test
  @DisplayName("the first-sighting signal under-reports distinct skiers while PFCOUNT does not")
  void firstSightingSignalUnderReportsAtRealisticCardinality() {
    LettuceConnectionFactory connections =
        new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
    connections.afterPropertiesSet();
    try {
      StringRedisTemplate redis = new StringRedisTemplate(connections);
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      ConsumerMetrics metrics = new ConsumerMetrics(registry);
      RedisHyperLogLogCounter counter =
          new RedisHyperLogLogCounter(redis, new DiscardingDynamoDb(), metrics);

      int reportedNew = 0;
      for (int skierId = 1; skierId <= DISTINCT_SKIERS; skierId++) {
        if (counter.observe(event(skierId))) {
          reportedNew++;
        }
      }

      long estimate = counter.estimate(RESORT_DAY);
      double counterValue = registry.get("skier.cardinality.unique.observed").counter().count();
      double suppressed = registry.get("skier.cardinality.duplicate.suppressed").counter().count();

      assertThat(estimate)
          .as("PFCOUNT uses the whole register array; 0.81% standard error, so 2% is generous")
          .isBetween((long) (DISTINCT_SKIERS * 0.98), (long) (DISTINCT_SKIERS * 1.02));

      assertThat(reportedNew)
          .as("PFADD's changed-a-register signal misses new identities as the sketch fills")
          .isLessThan((int) (DISTINCT_SKIERS * 0.95))
          .isGreaterThan((int) (DISTINCT_SKIERS * 0.5));

      assertThat(counterValue).isEqualTo(reportedNew);

      assertThat(suppressed).isEqualTo(DISTINCT_SKIERS - reportedNew);

      assertThat(reportedNew).isNotEqualTo(DISTINCT_SKIERS);
    } finally {
      connections.destroy();
    }
  }
}
