package skiers.health;

import java.time.Duration;
import java.time.Instant;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import skiers.Constants;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

@Component("dynamoDb")
public class DynamoDbHealthIndicator implements HealthIndicator {

  private final DynamoDbClient dynamoDb;

  public DynamoDbHealthIndicator(DynamoDbClient dynamoDb) {
    this.dynamoDb = dynamoDb;
  }

  @Override
  public Health health() {
    Instant startedAt = Instant.now();
    try {
      DescribeTableResponse result =
          dynamoDb.describeTable(
              DescribeTableRequest.builder().tableName(Constants.TARGET_TABLE_NAME).build());
      // The raw string: v2 maps a status it does not recognise to UNKNOWN_TO_SDK_VERSION.
      String tableStatus = result.table().tableStatusAsString();
      long latencyMs = Duration.between(startedAt, Instant.now()).toMillis();

      Health.Builder builder = "ACTIVE".equals(tableStatus) ? Health.up() : Health.down();
      return builder
          .withDetail("table", Constants.TARGET_TABLE_NAME)
          .withDetail("tableStatus", tableStatus)
          .withDetail("itemCount", result.table().itemCount())
          .withDetail("latencyMs", latencyMs)
          .build();
    } catch (Exception e) {
      return Health.down()
          .withDetail("table", Constants.TARGET_TABLE_NAME)
          .withDetail("error", e.getClass().getSimpleName() + ": " + e.getMessage())
          .build();
    }
  }
}
