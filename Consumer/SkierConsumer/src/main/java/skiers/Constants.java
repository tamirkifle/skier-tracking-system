package skiers;

public class Constants {
  // RabbitMQ Constants
  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";
  public static final String LIFT_RIDE_EXCHANGE = "liftRideExchange";
  public static final String LIFT_RIDE_ROUTING_KEY = "lift.ride";
  public static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";

  // Consumer Configuration
  public static final int CONCURRENT_CONSUMERS = 200;
  public static final int MAX_CONCURRENT_CONSUMERS = 200;
  public static final int PREFETCH_COUNT = 2000;
  public static final int MAX_DB_CONNECTION = 500;
  public static final int DB_CONNECTION_TIMEOUT = 10_000;
  public static final int DB_REQUEST_TIMEOUT = 20_000;

  public static final int MAX_QUEUE_SIZE = 50_000;
  // Error Messages
  public static final String INVALID_SKIER_ID = "Invalid skier ID";

}