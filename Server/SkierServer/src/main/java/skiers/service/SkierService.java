package skiers.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import skiers.Constants;
import skiers.model.ResortSkierCount;
import skiers.model.SkierVertical;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * Read path over DynamoDB, fronted by the cache. Spring's cache interceptor runs outside the
 * Resilience4j one, so a cache hit costs no circuit-breaker call and an open breaker serves warm.
 */
@Service
public class SkierService {

  private static final Logger logger = LoggerFactory.getLogger(SkierService.class);

  private static final String CIRCUIT = "dynamodb";

  private final DynamoDbClient dynamoDb;

  public SkierService(DynamoDbClient dynamoDb) {
    this.dynamoDb = dynamoDb;
  }

  /**
   * {@code unless} matters as much as the TTL. In an eventually-consistent pipeline a zero means
   * the consumer has not persisted the rides yet, and caching it pins that for the whole TTL.
   */
  @Cacheable(
      value = "skierDayVertical",
      key = "{#resortID, #seasonID, #dayID, #skierID}",
      unless = "#result == null || #result.totalVertical == 0")
  @CircuitBreaker(name = CIRCUIT)
  public SkierVertical getSkierData(
      String resortID, String seasonID, String dayID, String skierID) {
    return new SkierVertical(
        resortID, seasonID, dayID, skierID, fetchDayVertical(resortID, seasonID, dayID, skierID));
  }

  /** Not cached when zero: the counter row does not exist until the first skier is counted. */
  @Cacheable(
      value = "resortSkierCount",
      key = "{#resortID, #seasonID, #dayID}",
      unless = "#result == null || #result.uniqueNumSkiers == 0")
  @CircuitBreaker(name = CIRCUIT)
  public ResortSkierCount getUniqueSkiersCount(String resortID, String seasonID, String dayID) {
    return new ResortSkierCount(resortID, fetchUniqueSkiersCount(resortID, seasonID, dayID));
  }

  @Cacheable(
      value = "skierResortTotals",
      key = "{#skierID, #resort, #season}",
      unless = "#result == null || #result['resorts'].isEmpty()")
  @CircuitBreaker(name = CIRCUIT)
  public Map<String, Object> getSkierResortTotals(String skierID, String resort, String season) {
    Map<String, Integer> seasonTotals = fetchSkierResortTotals(skierID, resort, season);

    List<Map<String, Object>> resorts = new ArrayList<>(seasonTotals.size());
    seasonTotals.forEach(
        (seasonId, total) -> {
          Map<String, Object> entry = new LinkedHashMap<>(2);
          entry.put("seasonID", seasonId);
          entry.put("totalVert", total);
          resorts.add(entry);
        });

    Map<String, Object> response = new LinkedHashMap<>(1);
    response.put("resorts", resorts);
    return response;
  }

  private int fetchDayVertical(String resortID, String seasonID, String dayID, String skierID) {
    QueryRequest request =
        QueryRequest.builder()
            .tableName(Constants.TARGET_TABLE_NAME)
            .keyConditionExpression(
                Constants.ATTR_SKIER_ID + " = :skierId AND begins_with(#sortKey, :prefix)")
            .expressionAttributeNames(Map.of("#sortKey", Constants.ATTR_SORT_KEY))
            .expressionAttributeValues(
                Map.of(
                    ":skierId",
                    AttributeValue.fromS(skierID),
                    ":prefix",
                    AttributeValue.fromS(resortID + '#' + seasonID + '#' + dayID + '#')))
            .build();

    return sumVertical(request);
  }

  private int fetchUniqueSkiersCount(String resortID, String seasonID, String dayID) {
    String key = resortID + '#' + seasonID + '#' + dayID;
    GetItemResponse response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(Constants.SKIER_COUNTS_TABLE)
                .key(Map.of(Constants.ATTR_RESORT_SEASON_DAY, AttributeValue.fromS(key)))
                .build());

    // hasItem() rather than a null test on item(): SDK v2 reports an absent item as an empty map,
    // so a null check compiles, never fires, and dereferences the empty map one line later.
    if (!response.hasItem()) {
      return 0;
    }
    AttributeValue count = response.item().get(Constants.ATTR_UNIQUE_SKIER_COUNT);
    return count == null ? 0 : readInt(count);
  }

  private Map<String, Integer> fetchSkierResortTotals(
      String skierID, String resort, String season) {
    Map<String, String> names = new HashMap<>(2);
    names.put("#pk", Constants.ATTR_RESORT_SKIER);

    Map<String, AttributeValue> values = new HashMap<>(2);
    values.put(":pk", AttributeValue.fromS(resort + '#' + skierID));

    String condition = "#pk = :pk";
    if (season != null && !season.isBlank()) {
      names.put("#sk", Constants.ATTR_SEASON_DAY);
      values.put(":seasonPrefix", AttributeValue.fromS(season + '#'));
      condition += " AND begins_with(#sk, :seasonPrefix)";
    }

    QueryRequest request =
        QueryRequest.builder()
            .tableName(Constants.TARGET_TABLE_NAME)
            .indexName(Constants.CS_INDEX)
            .keyConditionExpression(condition)
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .build();

    Map<String, Integer> totals = new LinkedHashMap<>();
    forEachPage(request, items -> accumulateBySeason(items, totals, season));
    return totals;
  }

  private void accumulateBySeason(
      List<Map<String, AttributeValue>> items,
      Map<String, Integer> totals,
      String requestedSeason) {
    for (Map<String, AttributeValue> item : items) {
      AttributeValue vertical = item.get(Constants.ATTR_VERTICAL);
      if (vertical == null) {
        continue;
      }
      AttributeValue sortKey = item.get(Constants.ATTR_SEASON_DAY);
      String seasonId =
          sortKey != null && sortKey.s() != null ? sortKey.s().split("#", 2)[0] : requestedSeason;
      if (seasonId == null) {
        continue;
      }
      totals.merge(seasonId, readInt(vertical), Integer::sum);
    }
  }

  private int sumVertical(QueryRequest request) {
    int[] total = {0};
    forEachPage(
        request,
        items -> {
          for (Map<String, AttributeValue> item : items) {
            AttributeValue vertical = item.get(Constants.ATTR_VERTICAL);
            if (vertical != null) {
              total[0] += readInt(vertical);
            }
          }
        });
    return total[0];
  }

  /**
   * Walks every page of a query. DynamoDB caps a {@code Query} response at 1 MB, so reading only
   * the first page undercounts any skier whose history exceeds it. Pagination ends on an empty
   * {@code lastEvaluatedKey}; SDK v2 uses that in place of a null one. Failures propagate, because
   * returning the partial sum turns a DynamoDB outage into a confidently wrong number.
   */
  private void forEachPage(
      QueryRequest request, Consumer<List<Map<String, AttributeValue>>> pageConsumer) {
    Map<String, AttributeValue> startKey = null;
    int pages = 0;
    do {
      QueryRequest page =
          startKey == null ? request : request.toBuilder().exclusiveStartKey(startKey).build();
      QueryResponse response = dynamoDb.query(page);
      pageConsumer.accept(response.items());
      startKey = response.lastEvaluatedKey();
      pages++;
    } while (startKey != null && !startKey.isEmpty());

    if (pages > 1 && logger.isDebugEnabled()) {
      logger.debug("Query on {} spanned {} page(s)", request.indexName(), pages);
    }
  }

  private static int readInt(AttributeValue value) {
    String raw = value.n() != null ? value.n() : value.s();
    if (raw == null) {
      return 0;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      logger.warn("Ignoring non-numeric attribute value {}", raw);
      return 0;
    }
  }
}
