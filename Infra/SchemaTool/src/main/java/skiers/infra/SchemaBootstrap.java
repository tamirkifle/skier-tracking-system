package skiers.infra;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

/**
 * Applies {@link DynamoDbSchema} to a DynamoDB endpoint. Creation is idempotent, so it can run at
 * container start, on every deploy and repeatedly in a test without a guard. Drift on an existing
 * table is reported and exits non-zero rather than reconciled: adding a GSI to a populated table is
 * a long backfill and a cost decision.
 *
 * <pre>
 *   java -jar skier-schema-tool.jar                          # real AWS, region from env
 *   java -jar skier-schema-tool.jar http://localhost:4566    # LocalStack
 * </pre>
 */
public final class SchemaBootstrap {

  private static final Logger logger = LoggerFactory.getLogger(SchemaBootstrap.class);

  private static final Duration ACTIVE_TIMEOUT = Duration.ofMinutes(2);

  private final DynamoDbClient client;

  public SchemaBootstrap(DynamoDbClient client) {
    this.client = client;
  }

  /** Creates every missing table, waits for {@code ACTIVE}, then applies TTL. */
  public Result apply() {
    List<String> created = new ArrayList<>();
    List<String> existing = new ArrayList<>();
    List<String> drift = new ArrayList<>();

    for (CreateTableRequest request : DynamoDbSchema.tables()) {
      String tableName = request.tableName();
      DescribeTableResponse describe = describe(tableName);

      if (describe == null) {
        try {
          client.createTable(request);
          created.add(tableName);
          logger.info("Created table {}", tableName);
        } catch (ResourceInUseException raced) {
          // ResourceInUse means another bootstrapper won the race, which counts as success.
          existing.add(tableName);
          logger.info("Table {} was created concurrently", tableName);
        }
      } else {
        existing.add(tableName);
        drift.addAll(compare(request, describe));
      }

      awaitActive(tableName);

      TimeToLiveSpecification ttl = DynamoDbSchema.timeToLiveFor(tableName);
      if (ttl != null) {
        ensureTimeToLive(tableName, ttl);
      }
    }

    return new Result(created, existing, drift);
  }

  /** {@code UpdateTimeToLive} fails rather than no-opping when TTL is already set, so check. */
  private void ensureTimeToLive(String tableName, TimeToLiveSpecification desired) {
    DescribeTimeToLiveResponse current =
        client.describeTimeToLive(DescribeTimeToLiveRequest.builder().tableName(tableName).build());

    boolean alreadyEnabled =
        current.timeToLiveDescription() != null
            && current.timeToLiveDescription().timeToLiveStatus() == TimeToLiveStatus.ENABLED
            && desired.attributeName().equals(current.timeToLiveDescription().attributeName());

    if (alreadyEnabled) {
      logger.info("TTL already enabled on {} ({})", tableName, desired.attributeName());
      return;
    }

    client.updateTimeToLive(
        UpdateTimeToLiveRequest.builder()
            .tableName(tableName)
            .timeToLiveSpecification(desired)
            .build());
    logger.info("Enabled TTL on {} ({})", tableName, desired.attributeName());
  }

  /** Reports differences without correcting them. */
  private List<String> compare(CreateTableRequest expected, DescribeTableResponse actual) {
    List<String> differences = new ArrayList<>();
    String tableName = expected.tableName();

    if (!expected.keySchema().equals(actual.table().keySchema())) {
      differences.add(
          tableName
              + ": key schema is "
              + describeKeys(actual.table().keySchema())
              + ", expected "
              + describeKeys(expected.keySchema()));
    }

    List<String> expectedIndexes =
        expected.globalSecondaryIndexes().stream()
            .map(index -> index.indexName())
            .sorted()
            .toList();
    List<String> actualIndexes =
        actual.table().hasGlobalSecondaryIndexes()
            ? actual.table().globalSecondaryIndexes().stream()
                .map(index -> index.indexName())
                .sorted()
                .toList()
            : List.of();

    if (!expectedIndexes.equals(actualIndexes)) {
      differences.add(
          tableName + ": indexes are " + actualIndexes + ", expected " + expectedIndexes);
    }

    return differences;
  }

  private static String describeKeys(
      List<software.amazon.awssdk.services.dynamodb.model.KeySchemaElement> keys) {
    return keys.stream()
        .map(key -> key.attributeName() + '(' + key.keyTypeAsString() + ')')
        .toList()
        .toString();
  }

  private DescribeTableResponse describe(String tableName) {
    try {
      return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    } catch (ResourceNotFoundException absent) {
      return null;
    }
  }

  /** {@code CreateTable} returns before the table is usable; writing then gets a not-found. */
  private void awaitActive(String tableName) {
    Instant deadline = Instant.now().plus(ACTIVE_TIMEOUT);
    while (Instant.now().isBefore(deadline)) {
      DescribeTableResponse describe = describe(tableName);
      if (describe != null
          && describe.table().tableStatus() == TableStatus.ACTIVE
          && indexesActive(describe)) {
        return;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted waiting for " + tableName, e);
      }
    }
    throw new IllegalStateException(
        "Table " + tableName + " did not become ACTIVE within " + ACTIVE_TIMEOUT);
  }

  private static boolean indexesActive(DescribeTableResponse describe) {
    if (!describe.table().hasGlobalSecondaryIndexes()) {
      return true;
    }
    return describe.table().globalSecondaryIndexes().stream()
        .allMatch(
            index ->
                index.indexStatus()
                    == software.amazon.awssdk.services.dynamodb.model.IndexStatus.ACTIVE);
  }

  public record Result(List<String> created, List<String> existing, List<String> drift) {}
}
