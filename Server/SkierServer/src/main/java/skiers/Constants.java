package skiers;

/** Values fixed by the data model, the API contract, or the broker topology. */
public final class Constants {

  private Constants() {}

  public static final int MAX_RATE = 8000;

  public static final int TARGET_QUEUE_SIZE = 150;
  public static final int MAX_QUEUE_SIZE = 200;
  public static final int MIN_QUEUE_SIZE = 100;

  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  public static final String HEADER_EVENT_ID = "x-event-id";

  /** Epoch millis handed to the broker; the AMQP timestamp property is whole seconds. */
  public static final String HEADER_PUBLISHED_AT = "x-published-at";

  public static final int MIN_RESORT_ID = 1;
  public static final int MAX_RESORT_ID = 10;
  public static final int SEASON_ID = 2025;
  public static final int DAY_ID = 1;
  public static final int MIN_SKIER_ID = 1;
  public static final int MAX_SKIER_ID = 100000;
  public static final int MIN_LIFT_ID = 1;
  public static final int MAX_LIFT_ID = 40;
  public static final int MIN_TIME = 1;
  public static final int MAX_TIME = 360;

  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;

  public static final String TARGET_TABLE_NAME = "LiftRides";
  public static final String SKIER_COUNTS_TABLE = "SkierCounts";
  public static final String SKIER_TRACKING_TABLE = "SkierTracking";

  public static final String SSD_INDEX = "SSD-Index";
  public static final String RD_INDEX = "RD-Index";
  public static final String CS_INDEX = "CS-Index";

  public static final String ATTR_SKIER_ID = "skierID";
  public static final String ATTR_SORT_KEY = "resortID#seasonID#dayID#timestamp";
  public static final String ATTR_RESORT_SKIER = "resortID#skierID";
  public static final String ATTR_SEASON_DAY = "seasonID#dayID";
  public static final String ATTR_RESORT_SEASON_DAY = "resortSeasonDay";
  public static final String ATTR_VERTICAL = "vertical";
  public static final String ATTR_UNIQUE_SKIER_COUNT = "uniqueSkierCount";
}
