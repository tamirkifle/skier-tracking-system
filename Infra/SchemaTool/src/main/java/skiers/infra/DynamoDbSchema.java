package skiers.infra;

import java.util.List;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * The DynamoDB schema as data, so the integration suite, Compose and a deployment apply the same
 * definition and a test cannot drift from production. Composite keys are {@code '#'}-joined
 * literals, because DynamoDB cannot compute a key from other attributes.
 */
public final class DynamoDbSchema {

  private DynamoDbSchema() {}

  public static final String LIFT_RIDES = "LiftRides";
  public static final String SKIER_COUNTS = "SkierCounts";
  public static final String SKIER_TRACKING = "SkierTracking";

  public static final String SSD_INDEX = "SSD-Index";
  public static final String RD_INDEX = "RD-Index";
  public static final String CS_INDEX = "CS-Index";

  public static final String SKIER_ID = "skierID";
  public static final String SORT_KEY = "resortID#seasonID#dayID#timestamp";
  public static final String SKIER_SEASON = "skierID#seasonID";
  public static final String DAY_ID = "dayID";
  public static final String RESORT_DAY = "resortID#dayID";
  public static final String RESORT_SKIER = "resortID#skierID";
  public static final String SEASON_DAY = "seasonID#dayID";
  public static final String RESORT_SEASON_DAY = "resortSeasonDay";
  public static final String SKIER_KEY = "skierKey";

  /** DynamoDB applies TTL to an item attribute by name, separately from table creation. */
  public static final String SKIER_TRACKING_TTL_ATTRIBUTE = "expiresAt";

  public static List<CreateTableRequest> tables() {
    return List.of(liftRides(), skierCounts(), skierTracking());
  }

  /** Partitioned by skier: a resort partition key would concentrate every write on ten keys. */
  private static CreateTableRequest liftRides() {
    return CreateTableRequest.builder()
        .tableName(LIFT_RIDES)
        .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
        .keySchema(
            KeySchemaElement.builder().attributeName(SKIER_ID).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build())
        .attributeDefinitions(
            string(SKIER_ID),
            string(SORT_KEY),
            string(SKIER_SEASON),
            string(DAY_ID),
            string(RESORT_DAY),
            string(RESORT_SKIER),
            string(SEASON_DAY))
        .globalSecondaryIndexes(ssdIndex(), rdIndex(), csIndex())
        .build();
  }

  /** Which days a skier skied in a season. Index write cost scales with the projection. */
  private static GlobalSecondaryIndex ssdIndex() {
    return GlobalSecondaryIndex.builder()
        .indexName(SSD_INDEX)
        .keySchema(
            KeySchemaElement.builder().attributeName(SKIER_SEASON).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(DAY_ID).keyType(KeyType.RANGE).build())
        .projection(
            Projection.builder()
                .projectionType(ProjectionType.INCLUDE)
                .nonKeyAttributes("vertical", "liftID", "resortID")
                .build())
        .build();
  }

  /** Who was at a resort on a day. {@code KEYS_ONLY}: the keys are the whole answer. */
  private static GlobalSecondaryIndex rdIndex() {
    return GlobalSecondaryIndex.builder()
        .indexName(RD_INDEX)
        .keySchema(
            KeySchemaElement.builder().attributeName(RESORT_DAY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SKIER_ID).keyType(KeyType.RANGE).build())
        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
        .build();
  }

  /** Partitioned by {@code resort#skier}; {@code resort#season} would be one hot partition. */
  private static GlobalSecondaryIndex csIndex() {
    return GlobalSecondaryIndex.builder()
        .indexName(CS_INDEX)
        .keySchema(
            KeySchemaElement.builder().attributeName(RESORT_SKIER).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SEASON_DAY).keyType(KeyType.RANGE).build())
        .projection(
            Projection.builder()
                .projectionType(ProjectionType.INCLUDE)
                .nonKeyAttributes("vertical", "liftID")
                .build())
        .build();
  }

  private static CreateTableRequest skierCounts() {
    return CreateTableRequest.builder()
        .tableName(SKIER_COUNTS)
        .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
        .keySchema(
            KeySchemaElement.builder()
                .attributeName(RESORT_SEASON_DAY)
                .keyType(KeyType.HASH)
                .build())
        .attributeDefinitions(string(RESORT_SEASON_DAY))
        .build();
  }

  /** One sentinel per {@code resort#season#day#skier}, put under {@code attribute_not_exists}. */
  private static CreateTableRequest skierTracking() {
    return CreateTableRequest.builder()
        .tableName(SKIER_TRACKING)
        .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
        .keySchema(
            KeySchemaElement.builder().attributeName(SKIER_KEY).keyType(KeyType.HASH).build())
        .attributeDefinitions(string(SKIER_KEY))
        .build();
  }

  private static AttributeDefinition string(String name) {
    return AttributeDefinition.builder()
        .attributeName(name)
        .attributeType(ScalarAttributeType.S)
        .build();
  }
}
