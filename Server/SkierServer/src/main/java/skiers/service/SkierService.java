package skiers.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import skiers.Constants;
import skiers.model.SkierVertical;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
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
