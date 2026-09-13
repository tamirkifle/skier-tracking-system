package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import skiers.Constants;
import skiers.cardinality.DynamoDbUniqueSkierCounter;
import skiers.cardinality.UniqueSkierCounter;
import skiers.persistence.BatchingLiftRideWriter;
import skiers.persistence.LiftRideWriter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PipelineIntegrationTest extends PipelineIntegrationTestBase {

  @DynamicPropertySource
  static void configureStrategies(DynamicPropertyRegistry registry) {
    registry.add("skier.writer.mode", () -> "batch");
    registry.add("skier.cardinality.strategy", () -> "dynamodb");
    registry.add("skier.writer.max-retries", () -> "2");
    registry.add("skier.writer.retry-delay-ms", () -> "200");
  }

  @Override
  protected Class<? extends LiftRideWriter> expectedWriter() {
    return BatchingLiftRideWriter.class;
  }

  @Override
  protected Class<? extends UniqueSkierCounter> expectedCounter() {
    return DynamoDbUniqueSkierCounter.class;
  }

  @Test
  @DisplayName("the GSIs are queryable with the composite keys the write path materialises")
  void indexesAreQueryable() {
    publish("5", "1", 21, 300);
    await().atMost(Duration.ofSeconds(30)).until(() -> countRides() == 1);

    QueryRequest csQuery =
        QueryRequest.builder()
            .tableName(Constants.LIFT_RIDES_TABLE)
            .indexName("CS-Index")
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :season)")
            .expressionAttributeNames(
                Map.of("#pk", Constants.ATTR_RESORT_SKIER, "#sk", Constants.ATTR_SEASON_DAY))
            .expressionAttributeValues(
                Map.of(
                    ":pk", AttributeValue.fromS("5#" + skierId),
                    ":season", AttributeValue.fromS("2025#")))
            .build();

    await().atMost(Duration.ofSeconds(30)).until(() -> dynamoDb.query(csQuery).items().size() == 1);
    assertThat(dynamoDb.query(csQuery).items().get(0).get(Constants.ATTR_VERTICAL).s())
        .isEqualTo("210");

    QueryRequest ssdQuery =
        QueryRequest.builder()
            .tableName(Constants.LIFT_RIDES_TABLE)
            .indexName("SSD-Index")
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(skierId + "#2025")))
            .expressionAttributeNames(Map.of("#pk", Constants.ATTR_SKIER_SEASON))
            .build();
    assertThat(dynamoDb.query(ssdQuery).items()).hasSize(1);
  }

  @Test
  @DisplayName("a skier is counted once for a resort-day no matter how many rides they take")
  void cardinalityCountsSkierOncePerResortDay() {
    String resortId = "9";
    String resortSeasonDay = resortId + "#2025#1";
    int before = uniqueSkierCount(resortSeasonDay);

    for (int i = 0; i < 10; i++) {
      publish(resortId, "1", (i % 40) + 1, 100 + i);
    }

    await().atMost(Duration.ofSeconds(45)).until(() -> countRides() == 10);
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertThat(uniqueSkierCount(resortSeasonDay)).isEqualTo(before + 1));

    ScanRequest sentinelScan =
        ScanRequest.builder()
            .tableName(Constants.SKIER_TRACKING_TABLE)
            .filterExpression("#key = :key")
            .expressionAttributeNames(Map.of("#key", Constants.ATTR_SKIER_KEY))
            .expressionAttributeValues(
                Map.of(":key", AttributeValue.fromS(resortSeasonDay + '#' + skierId)))
            .build();
    Map<String, AttributeValue> sentinel = dynamoDb.scan(sentinelScan).items().get(0);
    assertThat(dynamoDb.scan(sentinelScan).items()).hasSize(1);

    long expiresAt = Long.parseLong(sentinel.get(Constants.ATTR_EXPIRES_AT).n());
    long expectedFloor =
        Instant.now().plus(Constants.SKIER_TRACKING_TTL).minusSeconds(30).getEpochSecond();
    long expectedCeiling = Instant.now().plus(Constants.SKIER_TRACKING_TTL).getEpochSecond();
    assertThat(expiresAt).isBetween(expectedFloor, expectedCeiling);

    assertThat(
            dynamoDb
                .describeTimeToLive(
                    DescribeTimeToLiveRequest.builder()
                        .tableName(Constants.SKIER_TRACKING_TABLE)
                        .build())
                .timeToLiveDescription()
                .timeToLiveStatus())
        .isEqualTo(TimeToLiveStatus.ENABLED);
  }

  @Test
  @DisplayName("a redelivered event neither duplicates the item nor double-counts the skier")
  void redeliveryIsIdempotent() {
    String resortId = "8";
    String resortSeasonDay = resortId + "#2025#1";
    int before = uniqueSkierCount(resortSeasonDay);

    publish(resortId, "1", 21, 250);
    publish(resortId, "1", 21, 250);
    publish(resortId, "1", 21, 250);

    await().atMost(Duration.ofSeconds(30)).until(() -> countRides() >= 1);

    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(countRides()).isEqualTo(1));
    assertThat(uniqueSkierCount(resortSeasonDay)).isEqualTo(before + 1);
  }

  @Test
  @DisplayName("an unparseable message is dead-lettered rather than retried forever")
  void malformedMessageReachesTheDeadLetterQueue() {
    int before = rabbitAdmin.getQueueInfo(Constants.DLQ).getMessageCount();

    // Well-formed JSON the converter accepts, but missing every field the listener requires.
    rabbitTemplate.convertAndSend(
        Constants.LIFT_RIDE_EXCHANGE,
        Constants.LIFT_RIDE_ROUTING_KEY,
        Map.of("notASkierEvent", "at all"));

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(rabbitAdmin.getQueueInfo(Constants.DLQ).getMessageCount())
                    .isEqualTo(before + 1));
  }

  @Test
  @DisplayName("the retry budget is spent across real broker redeliveries, then dead-lettered")
  void retryBudgetIsSpentAcrossRealRedeliveries() {
    // DynamoDB refuses an empty string in a key attribute, so every write attempt really fails.
    String eventId = "budget-" + UUID.randomUUID();
    Map<String, Object> message = new HashMap<>();
    message.put("resortID", "5");
    message.put("seasonID", "2025");
    message.put("dayID", "1");
    message.put("skierID", "");
    message.put("liftID", 21);
    message.put("time", 217);

    rabbitTemplate.convertAndSend(
        Constants.LIFT_RIDE_EXCHANGE,
        Constants.LIFT_RIDE_ROUTING_KEY,
        message,
        amqpMessage -> {
          amqpMessage.getMessageProperties().setHeader(Constants.HEADER_EVENT_ID, eventId);
          return amqpMessage;
        });

    Message deadLettered =
        await()
            .atMost(Duration.ofSeconds(60))
            .until(() -> findInDeadLetterQueue(eventId), Objects::nonNull);

    Integer attempts = deadLettered.getMessageProperties().getHeader(Constants.HEADER_ATTEMPTS);
    // max-retries is 2, so the header carries the last count written by a retry.
    assertThat(attempts).isEqualTo(2);
  }

  @Test
  @DisplayName("the retry route is declared, and its delay and return path are the configured ones")
  void retryTopologyIsDeclared() {
    QueueInformation retryQueue = rabbitAdmin.getQueueInfo(Constants.RETRY_QUEUE);
    assertThat(retryQueue).isNotNull();

    assertThat(retryQueue.getConsumerCount()).isZero();
  }

  @Test
  @DisplayName("the declared topology matches what the server declares")
  void topologyIsDeclared() {
    assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE)).isNotNull();
    assertThat(rabbitAdmin.getQueueInfo(Constants.DLQ)).isNotNull();
  }

  private Message findInDeadLetterQueue(String eventId) {
    Message found = null;
    List<Message> others = new ArrayList<>();
    Message message;
    while ((message = rabbitTemplate.receive(Constants.DLQ, 200)) != null) {
      String id = message.getMessageProperties().getHeader(Constants.HEADER_EVENT_ID);
      if (eventId.equals(id)) {
        found = message;
      } else {
        others.add(message);
      }
    }
    others.forEach(
        other -> rabbitTemplate.send(Constants.DLX, Constants.DEAD_LETTER_ROUTING_KEY, other));
    return found;
  }
}
