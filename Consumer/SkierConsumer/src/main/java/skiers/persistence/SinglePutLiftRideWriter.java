package skiers.persistence;

import java.util.ArrayList;
import java.util.List;
import skiers.Constants;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** One {@code PutItem} per event, kept as the baseline arm of the write-strategy benchmark. */
public class SinglePutLiftRideWriter implements LiftRideWriter {

  private final DynamoDbClient dynamoDb;

  public SinglePutLiftRideWriter(DynamoDbClient dynamoDb) {
    this.dynamoDb = dynamoDb;
  }

  @Override
  public List<LiftRideEvent> write(List<LiftRideEvent> events) {
    List<LiftRideEvent> failed = new ArrayList<>(0);
    for (LiftRideEvent event : events) {
      try {
        dynamoDb.putItem(
            PutItemRequest.builder()
                .tableName(Constants.LIFT_RIDES_TABLE)
                .item(LiftRideItems.toAttributeMap(event))
                .build());
      } catch (RuntimeException e) {
        // Per-item isolation: one throttled key must not fail its neighbours in the same group.
        failed.add(event);
      }
    }
    return failed;
  }

  @Override
  public String name() {
    return "single-put";
  }

  @Override
  public int maxBatchSize() {
    return 1;
  }
}
