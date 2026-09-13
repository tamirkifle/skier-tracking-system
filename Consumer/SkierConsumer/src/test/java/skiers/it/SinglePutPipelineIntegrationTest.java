package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import skiers.Constants;
import skiers.cardinality.DynamoDbUniqueSkierCounter;
import skiers.cardinality.UniqueSkierCounter;
import skiers.persistence.LiftRideWriter;
import skiers.persistence.SinglePutLiftRideWriter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SinglePutPipelineIntegrationTest extends PipelineIntegrationTestBase {

  @DynamicPropertySource
  static void configureStrategies(DynamicPropertyRegistry registry) {
    registry.add("skier.writer.mode", () -> "single");
    registry.add("skier.cardinality.strategy", () -> "dynamodb");
  }

  @Override
  protected Class<? extends LiftRideWriter> expectedWriter() {
    return SinglePutLiftRideWriter.class;
  }

  @Override
  protected Class<? extends UniqueSkierCounter> expectedCounter() {
    return DynamoDbUniqueSkierCounter.class;
  }

  @Test
  @DisplayName("each event is written as its own item, not coalesced away")
  void everyEventBecomesItsOwnItem() {
    int events = 10;
    for (int i = 0; i < events; i++) {
      publish("5", "1", (i % 40) + 1, 400 + i);
    }

    await().atMost(Duration.ofSeconds(45)).until(() -> countRides() == events);

    assertThat(dynamoDb.query(ridesBySkier()).items())
        .extracting(item -> item.get(Constants.ATTR_SORT_KEY).s())
        .hasSize(events)
        .doesNotHaveDuplicates();

    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount() == 0);
    assertThat(processor.stagedCount()).isZero();
  }

  @Test
  @DisplayName("the cardinality counter still runs on this arm")
  void cardinalityIsCountedOnTheSinglePutPath() {
    String resortId = "7";
    String resortSeasonDay = resortId + "#2025#1";
    int before = uniqueSkierCount(resortSeasonDay);

    publish(resortId, "1", 21, 500);
    publish(resortId, "1", 22, 501);

    await().atMost(Duration.ofSeconds(30)).until(() -> countRides() == 2);
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertThat(uniqueSkierCount(resortSeasonDay)).isEqualTo(before + 1));

    assertThat(dynamoDb.query(ridesBySkier()).items())
        .allSatisfy(
            item ->
                assertThat(item.get(Constants.ATTR_RESORT_SEASON_DAY))
                    .isEqualTo(AttributeValue.fromS(resortSeasonDay)));
  }
}
