package skiers.cardinality;

import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import skiers.Constants;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Approximate cardinality: {@code PFADD} into a per-resort-day sketch, with the {@code PFCOUNT}
 * estimate mirrored into {@code SkierCounts} on cardinality growth so the read API has a row to
 * serve. Re-adding an identity cannot move the sketch, so a redelivery needs no coordination.
 */
public class RedisHyperLogLogCounter implements UniqueSkierCounter {

  private static final Logger logger = LoggerFactory.getLogger(RedisHyperLogLogCounter.class);

  private static final String KEY_PREFIX = "skiers:hll:";

  /** A season plus slack, so historical days stay queryable without unbounded key growth. */
  private static final Duration TTL = Duration.ofDays(400);

  private final StringRedisTemplate redis;
  private final DynamoDbClient dynamoDb;
  private final ConsumerMetrics metrics;

  public RedisHyperLogLogCounter(
      StringRedisTemplate redis, DynamoDbClient dynamoDb, ConsumerMetrics metrics) {
    this.redis = redis;
    this.dynamoDb = dynamoDb;
    this.metrics = metrics;
  }

  @Override
  public boolean observe(LiftRideEvent event) {
    String resortSeasonDay = event.resortSeasonDayKey();
    String key = KEY_PREFIX + resortSeasonDay;
    boolean isNew;
    try {
      Long changed = redis.opsForHyperLogLog().add(key, event.skierId());
      isNew = changed != null && changed > 0;
      if (isNew) {
        redis.expire(key, TTL);
        metrics.recordUniqueSkier();
      } else {
        metrics.recordDuplicateSuppressed();
      }
    } catch (RuntimeException e) {
      // The ride is already durable; a lost cardinality update degrades a dashboard only.
      logger.warn("HyperLogLog update failed for {}: {}", key, e.getMessage());
      return false;
    }

    if (isNew) {
      mirror(resortSeasonDay);
    }
    return isNew;
  }

  /** Publishes the estimate, conditional on an increase so a stale reader cannot lower it. */
  private void mirror(String resortSeasonDay) {
    try {
      long estimate = estimate(resortSeasonDay);
      metrics.recordCardinalityWriteRequest();
      metrics.recordCardinalityWriteItems(1);
      dynamoDb.updateItem(
          UpdateItemRequest.builder()
              .tableName(Constants.SKIER_COUNTS_TABLE)
              .key(Map.of(Constants.ATTR_RESORT_SEASON_DAY, AttributeValue.fromS(resortSeasonDay)))
              .updateExpression("SET " + Constants.ATTR_UNIQUE_SKIER_COUNT + " = :estimate")
              .conditionExpression(
                  "attribute_not_exists("
                      + Constants.ATTR_UNIQUE_SKIER_COUNT
                      + ") OR "
                      + Constants.ATTR_UNIQUE_SKIER_COUNT
                      + " < :estimate")
              .expressionAttributeValues(
                  Map.of(":estimate", AttributeValue.fromN(Long.toString(estimate))))
              .build());
    } catch (ConditionalCheckFailedException alreadyAhead) {
      logger.trace("Mirror for {} lost to a larger concurrent estimate", resortSeasonDay);
    } catch (RuntimeException e) {
      logger.warn("Mirroring the HyperLogLog estimate for {} failed: {}", resortSeasonDay, e);
    }
  }

  public long estimate(String resortSeasonDayKey) {
    Long size = redis.opsForHyperLogLog().size(KEY_PREFIX + resortSeasonDayKey);
    return size == null ? 0L : size;
  }

  @Override
  public String name() {
    return "redis-hyperloglog";
  }
}
