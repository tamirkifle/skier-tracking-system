package skiers;

public final class Constants {

  private Constants() {}

  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  public static final String LIFT_RIDES_TABLE = "LiftRides";

  public static final String ATTR_SKIER_ID = "skierID";
  public static final String ATTR_SORT_KEY = "resortID#seasonID#dayID#timestamp";
  public static final String ATTR_SKIER_SEASON = "skierID#seasonID";
  public static final String ATTR_RESORT_DAY = "resortID#dayID";
  public static final String ATTR_RESORT_SKIER = "resortID#skierID";
  public static final String ATTR_SEASON_DAY = "seasonID#dayID";
  public static final String ATTR_RESORT_SEASON_DAY = "resortSeasonDay";
  public static final String ATTR_RESORT_ID = "resortID";
  public static final String ATTR_SEASON_ID = "seasonID";
  public static final String ATTR_DAY_ID = "dayID";
  public static final String ATTR_LIFT_ID = "liftID";
  public static final String ATTR_TIMESTAMP = "timestamp";
  public static final String ATTR_VERTICAL = "vertical";

  public static final String HEADER_EVENT_ID = "x-event-id";

  public static final String HEADER_PUBLISHED_AT = "x-published-at";

  /** Vertical feet gained per lift ride: liftID x 10. */
  public static final int VERTICAL_FEET_PER_LIFT = 10;

  /** Ceiling imposed by the DynamoDB BatchWriteItem API. */
  public static final int MAX_BATCH_WRITE_ITEMS = 25;

  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;
}
