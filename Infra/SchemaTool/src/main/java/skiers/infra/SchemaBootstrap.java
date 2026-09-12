package skiers.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

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
}
