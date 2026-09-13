package skiers.infra;

import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Recomputes {@code SkierCounts.uniqueSkierCount} from the {@code SkierTracking} sentinels. One
 * sentinel is one distinct skier-day, so the count is derivable from them.
 *
 * <p>The scan is not a snapshot, so a sentinel claimed behind the cursor is missing from the
 * recount. {@code --apply} requires {@code --confirm-ingest-paused}, a claim the operator makes and
 * this tool cannot check. It never lowers a count: sentinels expire under TTL, so a season whose
 * sentinels are gone recounts to zero while the stored value is still correct.
 *
 * <pre>
 *   java -cp skier-schema-tool.jar skiers.infra.CardinalityRepair --endpoint http://localhost:4566
 *   java -cp skier-schema-tool.jar skiers.infra.CardinalityRepair --apply
 * </pre>
 */
public final class CardinalityRepair {

  private static final Logger logger = LoggerFactory.getLogger(CardinalityRepair.class);

  private static final String UNIQUE_SKIER_COUNT = "uniqueSkierCount";

  private final DynamoDbClient client;

  public CardinalityRepair(DynamoDbClient client) {
    this.client = client;
  }

  public record Finding(String resortSeasonDay, long stored, long recounted) {

    public long shortfall() {
      return recounted - stored;
    }

    public boolean needsRepair() {
      return recounted > stored;
    }
  }

  /** A full scan: nothing indexes {@code resortSeasonDay}, and the table is keyed by skierKey. */
  public Map<String, Finding> recount() {
    Map<String, Long> counts = new TreeMap<>();
    Map<String, AttributeValue> startKey = null;

    do {
      ScanRequest.Builder request =
          ScanRequest.builder()
              .tableName(DynamoDbSchema.SKIER_TRACKING)
              .projectionExpression("#rsd")
              .expressionAttributeNames(Map.of("#rsd", DynamoDbSchema.RESORT_SEASON_DAY));
      if (startKey != null && !startKey.isEmpty()) {
        request.exclusiveStartKey(startKey);
      }

      ScanResponse response = client.scan(request.build());
      for (Map<String, AttributeValue> item : response.items()) {
        AttributeValue resortSeasonDay = item.get(DynamoDbSchema.RESORT_SEASON_DAY);
        if (resortSeasonDay != null && resortSeasonDay.s() != null) {
          counts.merge(resortSeasonDay.s(), 1L, Long::sum);
        }
      }
      // v2 ends pagination with an empty map, not null, so ask rather than test the accessor.
      startKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
    } while (startKey != null && !startKey.isEmpty());

    Map<String, Finding> findings = new TreeMap<>();
    counts.forEach((key, recounted) -> findings.put(key, new Finding(key, stored(key), recounted)));
    return findings;
  }

  private long stored(String resortSeasonDay) {
    GetItemResponse response =
        client.getItem(
            GetItemRequest.builder()
                .tableName(DynamoDbSchema.SKIER_COUNTS)
                .key(
                    Map.of(DynamoDbSchema.RESORT_SEASON_DAY, AttributeValue.fromS(resortSeasonDay)))
                .consistentRead(true)
                .build());
    if (!response.hasItem()) {
      return 0;
    }
    AttributeValue count = response.item().get(UNIQUE_SKIER_COUNT);
    return count == null ? 0 : Long.parseLong(count.n());
  }
}
