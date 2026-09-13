package skiers.cardinality;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.HyperLogLogOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import skiers.Constants;
import skiers.ingest.DeliveryAck;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class UniqueSkierCounterTest {

  private SimpleMeterRegistry registry;
  private ConsumerMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new ConsumerMetrics(registry);
  }

  private double cardinalityWriteRequests() {
    return registry.get("skier.cardinality.write.requests").counter().count();
  }

  private double cardinalityWriteItems() {
    return registry.get("skier.cardinality.write.items").counter().count();
  }

  private static LiftRideEvent event(String skierId) {
    return new LiftRideEvent("evt", skierId, "5", "2025", "1", 21, 100, DeliveryAck.NONE);
  }

  @Nested
  @DisplayName("DynamoDB transactional sentinel")
  class Transactional {

    private DynamoDbClient dynamoDb;
    private DynamoDbUniqueSkierCounter counter;

    @BeforeEach
    void setUp() {
      dynamoDb = mock(DynamoDbClient.class);
      counter = new DynamoDbUniqueSkierCounter(dynamoDb, metrics);
    }

    /** The reason list is positional: index 0 is the sentinel put, index 1 the increment. */
    private static TransactionCanceledException sentinelAlreadyClaimed() {
      return TransactionCanceledException.builder()
          .message("Transaction cancelled")
          .cancellationReasons(
              CancellationReason.builder().code("ConditionalCheckFailed").build(),
              CancellationReason.builder().code("None").build())
          .build();
    }

    private static TransactionCanceledException throttled() {
      return TransactionCanceledException.builder()
          .message("Transaction cancelled")
          .cancellationReasons(
              CancellationReason.builder().code("None").build(),
              CancellationReason.builder().code("ThrottlingError").build())
          .build();
    }

    private void transactionSucceeds() {
      when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
          .thenReturn(TransactWriteItemsResponse.builder().build());
    }

    private TransactWriteItemsRequest captureTransaction() {
      ArgumentCaptor<TransactWriteItemsRequest> request =
          ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
      verify(dynamoDb).transactWriteItems(request.capture());
      return request.getValue();
    }

    @Test
    @DisplayName("the sentinel and the increment are one transaction, in that order")
    void firstSightingIsOneTransaction() {
      transactionSucceeds();

      assertThat(counter.observe(event("42"))).isTrue();

      List<TransactWriteItem> items = captureTransaction().transactItems();
      assertThat(items).hasSize(2);

      Put sentinel = items.get(0).put();
      assertThat(sentinel.tableName()).isEqualTo(Constants.SKIER_TRACKING_TABLE);
      assertThat(sentinel.conditionExpression())
          .isEqualTo("attribute_not_exists(" + Constants.ATTR_SKIER_KEY + ")");
      assertThat(sentinel.item().get(Constants.ATTR_SKIER_KEY).s()).isEqualTo("5#2025#1#42");

      Update increment = items.get(1).update();
      assertThat(increment.tableName()).isEqualTo(Constants.SKIER_COUNTS_TABLE);
      assertThat(increment.updateExpression())
          .isEqualTo("ADD " + Constants.ATTR_UNIQUE_SKIER_COUNT + " :one");
      // N carries the number as a string; sending it as S makes DynamoDB reject the ADD.
      assertThat(increment.expressionAttributeValues().get(":one").n()).isEqualTo("1");
    }

    @Test
    @DisplayName("a repeat sighting increments nothing")
    void repeatSightingIsSuppressed() {
      when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
          .thenThrow(sentinelAlreadyClaimed());

      assertThat(counter.observe(event("42"))).isFalse();
      assertThat(registry.get("skier.cardinality.duplicate.suppressed").counter().count())
          .isEqualTo(1.0);
    }

    @Test
    @DisplayName("a cancellation that is not the sentinel's condition is rethrown, not suppressed")
    void throttledTransactionIsRetriable() {
      when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
          .thenThrow(throttled());

      assertThatThrownBy(() -> counter.observe(event("42")))
          .isInstanceOf(TransactionCanceledException.class);
      assertThat(registry.get("skier.cardinality.duplicate.suppressed").counter().count()).isZero();
    }

    @Test
    @DisplayName("a failed update must not be settled, so the strategy requires its projection")
    void requiresItsProjection() {
      assertThat(counter.projectionRequired()).isTrue();
    }

    @Test
    @DisplayName("a redelivered event cannot double-count")
    void isIdempotentUnderRedelivery() {
      when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
          .thenReturn(TransactWriteItemsResponse.builder().build())
          .thenThrow(sentinelAlreadyClaimed());

      LiftRideEvent redelivered = event("42");
      assertThat(counter.observe(redelivered)).isTrue();
      assertThat(counter.observe(redelivered)).isFalse();

      assertThat(registry.get("skier.cardinality.unique.observed").counter().count())
          .isEqualTo(1.0);
    }

    @Test
    @DisplayName("a first sighting's sentinel carries a TTL so SkierTracking does not grow forever")
    void sentinelCarriesExpiry() {
      transactionSucceeds();

      Instant before = Instant.now().plus(Constants.SKIER_TRACKING_TTL);
      counter.observe(event("42"));
      Instant after = Instant.now().plus(Constants.SKIER_TRACKING_TTL);

      Put sentinel = captureTransaction().transactItems().get(0).put();
      long expiresAt = Long.parseLong(sentinel.item().get(Constants.ATTR_EXPIRES_AT).n());
      // Bounded rather than exact: the write path stamps Instant.now() at call time.
      assertThat(expiresAt).isBetween(before.getEpochSecond(), after.getEpochSecond());
    }

    @Test
    @DisplayName("an event costs one write request carrying two items, sighting or repeat")
    void costsOneRequestAndTwoItems() {
      transactionSucceeds();
      counter.observe(event("42"));

      assertThat(cardinalityWriteRequests()).isEqualTo(1.0);
      assertThat(cardinalityWriteItems()).isEqualTo(2.0);

      when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
          .thenThrow(sentinelAlreadyClaimed());
      counter.observe(event("42"));

      assertThat(cardinalityWriteRequests()).isEqualTo(2.0);
      assertThat(cardinalityWriteItems()).isEqualTo(4.0);
    }

    @Test
    @DisplayName("skiers are counted per resort-day, not globally")
    void scopesCountToResortDay() {
      transactionSucceeds();

      counter.observe(new LiftRideEvent("a", "42", "5", "2025", "1", 21, 100, DeliveryAck.NONE));
      counter.observe(new LiftRideEvent("b", "42", "7", "2025", "1", 21, 100, DeliveryAck.NONE));

      ArgumentCaptor<TransactWriteItemsRequest> request =
          ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
      verify(dynamoDb, times(2)).transactWriteItems(request.capture());
      assertThat(request.getAllValues())
          .extracting(
              value -> value.transactItems().get(0).put().item().get(Constants.ATTR_SKIER_KEY).s())
          .containsExactly("5#2025#1#42", "7#2025#1#42");
    }
  }

  @Nested
  @DisplayName("Redis HyperLogLog")
  class HyperLogLog {

    private HyperLogLogOperations<String, String> hll;
    private StringRedisTemplate redis;
    private DynamoDbClient dynamoDb;
    private RedisHyperLogLogCounter counter;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
      redis = mock(StringRedisTemplate.class);
      hll = mock(HyperLogLogOperations.class);
      dynamoDb = mock(DynamoDbClient.class);
      when(redis.opsForHyperLogLog()).thenReturn(hll);
      counter = new RedisHyperLogLogCounter(redis, dynamoDb, metrics);
    }

    @Test
    @DisplayName("PFADD reporting a change counts as a first sighting")
    void reportsFirstSighting() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L);

      assertThat(counter.observe(event("42"))).isTrue();
      verify(hll).add("skiers:hll:5#2025#1", "42");
      verify(redis).expire(anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("PFADD reporting no change is a repeat sighting and does not refresh the TTL")
    void suppressesRepeatSighting() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(0L);

      assertThat(counter.observe(event("42"))).isFalse();
      verify(redis, never()).expire(anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("re-adding an identity cannot move the estimate")
    void isStructurallyIdempotent() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L).thenReturn(0L);

      LiftRideEvent redelivered = event("42");
      assertThat(counter.observe(redelivered)).isTrue();
      assertThat(counter.observe(redelivered)).isFalse();
    }

    @Test
    @DisplayName("a Redis outage degrades the statistic rather than failing the event")
    void survivesRedisOutage() {
      when(hll.add(anyString(), any(String[].class)))
          .thenThrow(new org.springframework.dao.QueryTimeoutException("redis down"));

      assertThat(counter.observe(event("42"))).isFalse();
    }

    @Test
    @DisplayName("estimate reads the sketch for the requested resort-day")
    void readsEstimate() {
      when(hll.size("skiers:hll:5#2025#1")).thenReturn(98765L);
      assertThat(counter.estimate("5#2025#1")).isEqualTo(98765L);
    }

    @Test
    @DisplayName("a missing sketch estimates zero")
    void missingSketchIsZero() {
      when(hll.size(anyString())).thenReturn(null);
      assertThat(counter.estimate("5#2025#1")).isZero();
    }

    @Test
    @DisplayName("cardinality growth publishes PFCOUNT to the row the read API serves")
    void mirrorsEstimateToSkierCounts() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L);
      when(hll.size("skiers:hll:5#2025#1")).thenReturn(4321L);
      when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
          .thenReturn(UpdateItemResponse.builder().build());

      assertThat(counter.observe(event("42"))).isTrue();

      ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
      verify(dynamoDb).updateItem(update.capture());
      assertThat(update.getValue().tableName()).isEqualTo(Constants.SKIER_COUNTS_TABLE);
      assertThat(update.getValue().key().get(Constants.ATTR_RESORT_SEASON_DAY).s())
          .isEqualTo("5#2025#1");
      // SET, not ADD: the sketch already holds the whole count.
      assertThat(update.getValue().updateExpression())
          .isEqualTo("SET " + Constants.ATTR_UNIQUE_SKIER_COUNT + " = :estimate");
      assertThat(update.getValue().expressionAttributeValues().get(":estimate").n())
          .isEqualTo("4321");
      assertThat(update.getValue().conditionExpression())
          .isEqualTo("attribute_not_exists(uniqueSkierCount) OR uniqueSkierCount < :estimate");
    }

    @Test
    @DisplayName("a repeat sighting does not touch DynamoDB at all")
    void repeatSightingDoesNotMirror() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(0L);

      assertThat(counter.observe(event("42"))).isFalse();

      verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    @DisplayName("the mirror is counted as one DynamoDB write request, and a repeat costs none")
    void mirrorIsCountedAsAWriteRequest() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L).thenReturn(0L);
      when(hll.size(anyString())).thenReturn(4321L);
      when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
          .thenReturn(UpdateItemResponse.builder().build());

      LiftRideEvent redelivered = event("42");
      counter.observe(redelivered);
      counter.observe(redelivered);

      assertThat(cardinalityWriteRequests()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("a mirror that loses its condition still cost a write request")
    void mirrorRejectedByTheConditionIsStillCounted() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L);
      when(hll.size(anyString())).thenReturn(10L);
      when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
          .thenThrow(ConditionalCheckFailedException.builder().message("already ahead").build());

      counter.observe(event("42"));

      assertThat(cardinalityWriteRequests()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("a Redis outage costs no DynamoDB write request")
    void redisOutageIssuesNoWrite() {
      when(hll.add(anyString(), any(String[].class)))
          .thenThrow(new org.springframework.dao.QueryTimeoutException("redis down"));

      counter.observe(event("42"));

      assertThat(cardinalityWriteRequests()).isZero();
    }

    @Test
    @DisplayName("losing the monotone condition to a concurrent instance is not an error")
    void concurrentLargerEstimateWins() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L);
      when(hll.size(anyString())).thenReturn(10L);
      when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
          .thenThrow(ConditionalCheckFailedException.builder().message("already ahead").build());

      assertThat(counter.observe(event("42"))).isTrue();
    }

    @Test
    @DisplayName("a mirror failure degrades the published count rather than the sighting answer")
    void mirrorFailureDoesNotChangeTheFirstSightingAnswer() {
      when(hll.add(anyString(), any(String[].class))).thenReturn(1L);
      when(hll.size(anyString())).thenReturn(10L);
      when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
          .thenThrow(new IllegalStateException("dynamo down"));

      assertThat(counter.observe(event("42"))).isTrue();
    }
  }
}
