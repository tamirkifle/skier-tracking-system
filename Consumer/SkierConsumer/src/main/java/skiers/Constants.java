package skiers;

public final class Constants {

  private Constants() {}

  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  public static final String HEADER_EVENT_ID = "x-event-id";

  public static final String HEADER_PUBLISHED_AT = "x-published-at";

  /** Vertical feet gained per lift ride: liftID x 10. */
  public static final int VERTICAL_FEET_PER_LIFT = 10;

  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;
}
