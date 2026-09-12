package skiers.infra;

import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

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
}
