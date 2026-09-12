package skiers.infra;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
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

  private static AttributeDefinition string(String name) {
    return AttributeDefinition.builder()
        .attributeName(name)
        .attributeType(ScalarAttributeType.S)
        .build();
  }
}
