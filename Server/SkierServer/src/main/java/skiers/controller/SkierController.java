package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import skiers.model.LiftRide;
import skiers.service.RateLimiter;
import skiers.service.SkierService;
import skiers.Constants;

@RestController
@RequestMapping("/skiers")
public class SkierController {
  private static final Logger logger = LoggerFactory.getLogger(SkierController.class);

  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private RateLimiter rateLimiter;
  @Autowired private AmazonDynamoDB amazonDynamoDB;
  @Autowired private SkierService skierService;

  @PostMapping("/{resortID}/seasons/{seasonID}/days/{dayID}/skier/{skierID}")
  public ResponseEntity<?> addLiftRide(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID,
      @PathVariable String skierID,
      @RequestBody LiftRide liftRide) throws InterruptedException {

    if (!isValidPathParameters(resortID, seasonID, dayID, skierID)) {
      logger.warn("Invalid path parameters: resortID={}, seasonID={}, dayID={}, skierID={}", resortID, seasonID, dayID, skierID);
      return ResponseEntity.badRequest().body(Constants.INVALID_PATH_PARAMETERS);
    }

    if (!isValidLiftRide(liftRide)) {
      logger.warn("Invalid LiftRide data: {}", liftRide);
      return ResponseEntity.badRequest().body(Constants.INVALID_LIFT_RIDE_DATA);
    }

    Map<String, Object> message = Map.of(
        "resortID", resortID,
        "seasonID", seasonID,
        "dayID", dayID,
        "skierID", skierID,
        "liftID", liftRide.getLiftID(),
        "time", liftRide.getTime()
    );

    int retryCount = 0;
    while (retryCount < Constants.MAX_RETRIES) {
      if (rateLimiter.tryAcquire()) {
        rabbitTemplate.convertAndSend(Constants.LIFT_RIDE_EXCHANGE, Constants.LIFT_RIDE_ROUTING_KEY, message);
        logger.info("Successfully sent message to queue: {}", message);
        return ResponseEntity.ok().build();
      }
      Thread.sleep(Constants.RETRY_SLEEP_TIME_MS);
      retryCount++;
    }

    logger.error("Rate limit exceeded after {} retries", Constants.MAX_RETRIES);
    return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Constants.RATE_LIMIT_EXCEEDED);
  }

  /**
   * Get the total vertical for the skier for specified resort and optional season
   */
  @GetMapping("/{skierID}/vertical")
  public ResponseEntity<?> getSkierResortTotals(
      @PathVariable String skierID,
      @RequestParam(required = true) String resort,
      @RequestParam(required = false) String season) {

    try {
      // Validate inputs
      if (!isValidSkierResort(skierID, resort)) {
        logger.warn("Invalid input parameters: skierID={}, resort={}", skierID, resort);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(Map.of("message", "Invalid input parameters"));
      }

      // Use service layer with caching
      Map<String, Object> response = skierService.getSkierResortTotals(skierID, resort, season);
      
      // If no data found, return 404
      if (((java.util.List<?>) response.get("resorts")).isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("message", "No vertical data found for the specified parameters"));
      }

      return ResponseEntity.ok(response);

    } catch (Exception e) {
      logger.error("Error retrieving skier resort totals", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("message", "Error processing request: " + e.getMessage()));
    }
  }

  private boolean isValidPathParameters(String resortID, String seasonID, String dayID, String skierID) {
    try {
      int resort = Integer.parseInt(resortID);
      int season = Integer.parseInt(seasonID);
      int day = Integer.parseInt(dayID);
      int skier = Integer.parseInt(skierID);

      return resort >= Constants.MIN_RESORT_ID && resort <= Constants.MAX_RESORT_ID &&
          season == Constants.SEASON_ID &&
          skier >= Constants.MIN_SKIER_ID && skier <= Constants.MAX_SKIER_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private boolean isValidLiftRide(LiftRide liftRide) {
    if (liftRide == null) {
      return false;
    }
    Integer liftID = liftRide.getLiftID();
    Integer time = liftRide.getTime();

    return liftID != null && liftID >= Constants.MIN_LIFT_ID && liftID <= Constants.MAX_LIFT_ID &&
        time != null && time >= Constants.MIN_TIME && time <= Constants.MAX_TIME;
  }

  /**
   * Validate inputs for resort totals endpoint
   */
  private boolean isValidSkierResort(String skierID, String resort) {
    // Just check that skierID and resort are not null or empty
    return skierID != null && !skierID.trim().isEmpty() &&
        resort != null && !resort.trim().isEmpty();
  }

  @GetMapping("/dynamodb")
  public ResponseEntity<?> checkDynamoDBConnection() {
    try {
      // First check basic connectivity
      amazonDynamoDB.listTables(new ListTablesRequest().withLimit(1));

      // Then check if our target table exists
      boolean tableExists = false;
      try {
        amazonDynamoDB.describeTable(new DescribeTableRequest(Constants.TARGET_TABLE_NAME));
        tableExists = true;
      } catch (ResourceNotFoundException rnfe) {
        // Table doesn't exist, but the connection is still valid
      }

      Map<String, Object> response = new HashMap<>();
      response.put("status", "UP");

      // Include table existence in the message
      if (tableExists) {
        response.put("message", "Successfully connected to DynamoDB and found table: " + Constants.TARGET_TABLE_NAME);
      } else {
        response.put("message", "Successfully connected to DynamoDB but table not found: " + Constants.TARGET_TABLE_NAME);
      }

      return ResponseEntity.ok(response);
    } catch (Exception e) {
      Map<String, Object> response = new HashMap<>();
      response.put("status", "DOWN");
      response.put("message", "Failed to connect to DynamoDB");
      response.put("error", e.getMessage());
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }
  }

  @GetMapping("/describe-table")
  public ResponseEntity<?> describeTable() {
    try {
      DescribeTableResult result = amazonDynamoDB.describeTable(new DescribeTableRequest(Constants.TARGET_TABLE_NAME));
      // Log the GSI information
      for (GlobalSecondaryIndexDescription gsi : result.getTable().getGlobalSecondaryIndexes()) {
        logger.info("GSI Name: {}", gsi.getIndexName());
        logger.info("GSI Key Schema: {}", gsi.getKeySchema());
      }
      return ResponseEntity.ok(result.getTable());
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("message", "Error describing table: " + e.getMessage()));
    }
  }

  @GetMapping("/health")
  public String healthCheck() {
    logger.info("Health check endpoint called");
    return "OK";
  }
}
