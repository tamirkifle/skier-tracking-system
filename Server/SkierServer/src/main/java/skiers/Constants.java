package skiers;

/** Values fixed by the data model, the API contract, or the broker topology. */
public final class Constants {

  private Constants() {}

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
}
