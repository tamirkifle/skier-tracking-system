package skiers.cardinality;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skiers.Constants;
import skiers.metrics.ConsumerMetrics;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

/**
 * Exact cardinality: the sentinel put and the counter increment in one {@code TransactWriteItems}.
 *
 * <p>As two requests, a failure between them leaves a sentinel that blocks the increment forever,
 * because every later sighting loses its condition. A transaction bills twice the write units.
 */
public class DynamoDbUniqueSkierCounter implements UniqueSkierCounter {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbUniqueSkierCounter.class);

  private final DynamoDbClient dynamoDb;
  private final ConsumerMetrics metrics;

  public DynamoDbUniqueSkierCounter(DynamoDbClient dynamoDb, ConsumerMetrics metrics) {
    this.dynamoDb = dynamoDb;
    this.metrics = metrics;
  }

  private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  @Override
  public boolean observe(LiftRideEvent event) {
    String resortSeasonDay = event.resortSeasonDayKey();
    String skierKey = event.skierDayIdentity();

    // Billed whether or not the condition is won, so counted before the call.
    metrics.recordCardinalityWriteRequest();
    metrics.recordCardinalityWriteItems(2);

    try {
      dynamoDb.transactWriteItems(
          TransactWriteItemsRequest.builder()
              .transactItems(claimSentinel(skierKey, resortSeasonDay), increment(resortSeasonDay))
              .build());
    } catch (TransactionCanceledException cancelled) {
      if (isSentinelAlreadyClaimed(cancelled)) {
        metrics.recordDuplicateSuppressed();
        return false;
      }
      // A throttle or a conflicting transaction applied neither item, so it is retriable.
      // Returning normally would report a sighting that nothing wrote.
      throw cancelled;
    }

    metrics.recordUniqueSkier();
    if (logger.isTraceEnabled()) {
      logger.trace("First sighting of skier {} at {}", event.skierId(), resortSeasonDay);
    }
    return true;
  }

  private static TransactWriteItem claimSentinel(String skierKey, String resortSeasonDay) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(Constants.SKIER_TRACKING_TABLE)
                .item(
                    Map.of(
                        Constants.ATTR_SKIER_KEY,
                        AttributeValue.fromS(skierKey),
                        Constants.ATTR_RESORT_SEASON_DAY,
                        AttributeValue.fromS(resortSeasonDay),
                        Constants.ATTR_EXPIRES_AT,
                        AttributeValue.fromN(
                            Long.toString(
                                Instant.now()
                                    .plus(Constants.SKIER_TRACKING_TTL)
                                    .getEpochSecond()))))
                .conditionExpression("attribute_not_exists(" + Constants.ATTR_SKIER_KEY + ")")
                .build())
        .build();
  }

  /** {@code ADD} creates a missing item with the delta, so the first increment needs no branch. */
  private static TransactWriteItem increment(String resortSeasonDay) {
    return TransactWriteItem.builder()
        .update(
            Update.builder()
                .tableName(Constants.SKIER_COUNTS_TABLE)
                .key(
                    Map.of(Constants.ATTR_RESORT_SEASON_DAY, AttributeValue.fromS(resortSeasonDay)))
                .updateExpression("ADD " + Constants.ATTR_UNIQUE_SKIER_COUNT + " :one")
                .expressionAttributeValues(Map.of(":one", AttributeValue.fromN("1")))
                .build())
        .build();
  }

  /** Reason index 0 is the sentinel put; a failure anywhere else is retriable, not a duplicate. */
  private static boolean isSentinelAlreadyClaimed(TransactionCanceledException cancelled) {
    List<CancellationReason> reasons = cancelled.cancellationReasons();
    return !reasons.isEmpty() && CONDITIONAL_CHECK_FAILED.equals(reasons.get(0).code());
  }

  /** Nothing else holds the increment a failed update owed, so it must block settlement. */
  @Override
  public boolean projectionRequired() {
    return true;
  }

  @Override
  public String name() {
    return "dynamodb-transactional";
  }
}
