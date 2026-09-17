package skiers;

import java.time.Duration;

public final class Constants {

  private Constants() {}

  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  public static final String RETRY_QUEUE = "liftRideRetryQueue";

  public static final String RETRY_ROUTING_KEY = "lift.ride.retry";

  public static final String LIFT_RIDES_TABLE = "LiftRides";
  public static final String SKIER_COUNTS_TABLE = "SkierCounts";
  public static final String SKIER_TRACKING_TABLE = "SkierTracking";

  public static final String ATTR_SKIER_ID = "skierID";
  public static final String ATTR_SORT_KEY = "resortID#seasonID#dayID#minute#liftID";
  public static final String ATTR_SKIER_SEASON = "skierID#seasonID";
  public static final String ATTR_RESORT_DAY = "resortID#dayID";
  public static final String ATTR_RESORT_SKIER = "resortID#skierID";
  public static final String ATTR_SEASON_DAY = "seasonID#dayID";
  public static final String ATTR_RESORT_SEASON_DAY = "resortSeasonDay";
  public static final String ATTR_RESORT_ID = "resortID";
  public static final String ATTR_DAY_ID = "dayID";
  public static final String ATTR_LIFT_ID = "liftID";
  public static final String ATTR_VERTICAL = "vertical";
  public static final String ATTR_SKIER_KEY = "skierKey";
  public static final String ATTR_UNIQUE_SKIER_COUNT = "uniqueSkierCount";

  public static final String ATTR_EXPIRES_AT = "expiresAt";

  public static final Duration SKIER_TRACKING_TTL = Duration.ofDays(180);

  public static final String HEADER_EVENT_ID = "x-event-id";

  /** Attempt count, on the wire because the listener decodes a fresh event per delivery. */
  public static final String HEADER_ATTEMPTS = "x-attempts";

  public static final String HEADER_PUBLISHED_AT = "x-published-at";

  /** Vertical feet gained per lift ride: liftID x 10. */
  public static final int VERTICAL_FEET_PER_LIFT = 10;

  /** Ceiling imposed by the DynamoDB BatchWriteItem API. */
  public static final int MAX_BATCH_WRITE_ITEMS = 25;

  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;
}
