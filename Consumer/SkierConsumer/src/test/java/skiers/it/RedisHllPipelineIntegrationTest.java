package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import skiers.Constants;
import skiers.cardinality.RedisHyperLogLogCounter;
import skiers.cardinality.UniqueSkierCounter;
import skiers.persistence.BatchingLiftRideWriter;
import skiers.persistence.LiftRideWriter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RedisHllPipelineIntegrationTest extends PipelineIntegrationTestBase {

  /** Small enough that PFCOUNT is exact, so the assertion can name a number rather than a band. */
  private static final int DISTINCT_SKIERS = 5;

  private static final String RUN = Integer.toHexString(UUID.randomUUID().hashCode());
  private static final String COUNT_RESORT = "hll-count-" + RUN;
  private static final String TRACKING_RESORT = "hll-tracking-" + RUN;
  private static final String DAY = "1";

  @DynamicPropertySource
  static void configureStrategies(DynamicPropertyRegistry registry) {
    registry.add("skier.writer.mode", () -> "batch");
    registry.add("skier.cardinality.strategy", () -> "redis-hll");
  }

  @Override
  protected Class<? extends LiftRideWriter> expectedWriter() {
    return BatchingLiftRideWriter.class;
  }

  @Override
  protected Class<? extends UniqueSkierCounter> expectedCounter() {
    return RedisHyperLogLogCounter.class;
  }

  @Test
  @DisplayName("the unique-skiers row the read API serves is maintained under redis-hll")
  void uniqueSkierCountReachesTheTableTheReadApiServes() {
    for (int i = 0; i < DISTINCT_SKIERS; i++) {
      skierId = "9" + (100 + i);
      publish(COUNT_RESORT, DAY, 21, 300 + i);
    }

    await()
        .atMost(Duration.ofSeconds(45))
        .untilAsserted(
            () ->
                assertThat(uniqueSkierCount(COUNT_RESORT + "#2025#" + DAY))
                    .as(
                        "GET /resorts/%s/seasons/2025/day/%s/skiers reads this row",
                        COUNT_RESORT, DAY)
                    .isEqualTo(DISTINCT_SKIERS));
  }

  @Test
  @DisplayName("the tracking table stays empty, so the mirrored count is not the exact strategy")
  void doesNotFallBackToTheTrackingTable() {
    String resortSeasonDay = TRACKING_RESORT + "#2025#" + DAY;
    skierId = "9500";
    publish(TRACKING_RESORT, DAY, 22, 700);

    await().atMost(Duration.ofSeconds(45)).until(() -> uniqueSkierCount(resortSeasonDay) >= 1);

    assertThat(
            dynamoDb
                .getItem(
                    GetItemRequest.builder()
                        .tableName(Constants.SKIER_TRACKING_TABLE)
                        .key(
                            Map.of(
                                Constants.ATTR_SKIER_KEY,
                                AttributeValue.fromS(resortSeasonDay + "#" + skierId)))
                        .build())
                .hasItem())
        .isFalse();
  }
}
