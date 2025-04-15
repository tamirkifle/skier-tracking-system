package skiers;

public class Constants {
  // Rate Limiter Constants
  public static final int MAX_RATE = 8000;
  public static final int MIN_RATE = 100;
  public static final int TARGET_QUEUE_SIZE = 150;
  public static final int MAX_QUEUE_SIZE = 200;
  public static final int MIN_QUEUE_SIZE = 100;

  // Retry Logic Constants
  public static final int MAX_RETRIES = 100000; // Thread re-tries
  public static final int RETRY_SLEEP_TIME_MS = 5;

  // RabbitMQ Constants
  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  // Validation Constants
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

  // Error Messages
  public static final String INVALID_PATH_PARAMETERS = "Invalid path parameters";
  public static final String INVALID_LIFT_RIDE_DATA = "Invalid LiftRide data";
  public static final String RATE_LIMIT_EXCEEDED = "Rate limit exceeded";
  public static final String SERVER_ERROR = "Server Error: ";
  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;
}