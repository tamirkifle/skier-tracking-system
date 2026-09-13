package skiers.service;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
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

  private final DynamoDbClient dynamoDb;

  public SkierService(DynamoDbClient dynamoDb) {
    this.dynamoDb = dynamoDb;
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
