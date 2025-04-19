package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import java.util.HashMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import skiers.model.LiftRide;
import skiers.service.RateLimiter;
import skiers.Constants;

import java.util.Map;

@RestController
@RequestMapping("/skiers")
public class SkierController {
  private static final Logger logger = LoggerFactory.getLogger(SkierController.class);

  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private RateLimiter rateLimiter;
  @Autowired  private AmazonDynamoDB amazonDynamoDB;

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

  private boolean isValidPathParameters(String resortID, String seasonID, String dayID, String skierID) {
    try {
      int resort = Integer.parseInt(resortID);
      int season = Integer.parseInt(seasonID);
      int day = Integer.parseInt(dayID);
      int skier = Integer.parseInt(skierID);

      return resort >= Constants.MIN_RESORT_ID && resort <= Constants.MAX_RESORT_ID &&
          season == Constants.SEASON_ID &&
          day == Constants.DAY_ID &&
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
   * Get the total vertical for the skier for specified seasons at the specified resort
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

      // Compute the same hash as used in the data processor
      String shardedSkierId = skierID + "#" + (Math.abs(skierID.hashCode()) % 4);

      // Query on the primary hash key (skierID) and filter by resortID attribute
      Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
      expressionAttributeValues.put(":skier", new AttributeValue().withS(shardedSkierId));
      expressionAttributeValues.put(":resort", new AttributeValue().withS(resort));

      // Using the standalone resortID attribute for filtering
      QueryRequest queryRequest = new QueryRequest()
          .withTableName(Constants.TARGET_TABLE_NAME)
          .withKeyConditionExpression("skierID = :skier")
          .withFilterExpression("resortID = :resort")
          .withExpressionAttributeValues(expressionAttributeValues);

      // Map to store vertical by season
      Map<String, Integer> seasonVerticalMap = new HashMap<>();

      try {
        // Query and process results
        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        processVerticalBySeasons(queryResult.getItems(), seasonVerticalMap);

        // Handle pagination if there are more results
        while (queryResult.getLastEvaluatedKey() != null && !queryResult.getLastEvaluatedKey().isEmpty()) {
          queryRequest.withExclusiveStartKey(queryResult.getLastEvaluatedKey());
          queryResult = amazonDynamoDB.query(queryRequest);
          processVerticalBySeasons(queryResult.getItems(), seasonVerticalMap);
        }

        // If no data found, return 404
        if (seasonVerticalMap.isEmpty()) {
          return ResponseEntity.status(HttpStatus.NOT_FOUND)
              .body(Map.of("message", "No vertical data found for the specified parameters"));
        }

        // Create the response with the expected format
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> resorts = new ArrayList<>();

        // If season is specified, filter for just that season
        if (season != null && !season.isEmpty()) {
          if (seasonVerticalMap.containsKey(season)) {
            Map<String, Object> seasonData = new HashMap<>();
            seasonData.put("seasonID", season);
            seasonData.put("totalVert", seasonVerticalMap.get(season));
            resorts.add(seasonData);
          } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("message", "No vertical data found for the specified season"));
          }
        } else {
          // Add all seasons to the response
          for (Map.Entry<String, Integer> entry : seasonVerticalMap.entrySet()) {
            Map<String, Object> seasonData = new HashMap<>();
            seasonData.put("seasonID", entry.getKey());
            seasonData.put("totalVert", entry.getValue());
            resorts.add(seasonData);
          }
        }

        response.put("resorts", resorts);
        return ResponseEntity.ok(response);

      } catch (Exception e) {
        logger.error("Error retrieving skier resort totals", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of("message", "Error processing request: " + e.getMessage()));
      }
    } catch (Exception e) {
      logger.error("Error retrieving skier resort totals", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("message", "Error processing request: " + e.getMessage()));
    }
  }

  /**
   * Process items to calculate vertical by season
   */
  private void processVerticalBySeasons(List<Map<String, AttributeValue>> items, Map<String, Integer> seasonVerticalMap) {
    if (items != null) {
      for (Map<String, AttributeValue> item : items) {
        if (item.containsKey("vertical") && item.containsKey("seasonID")) {
          try {
            // Get the vertical value as a string
            int vertical = Integer.parseInt(item.get("vertical").getS());

            // Get the season ID directly
            String seasonID = item.get("seasonID").getS();

            // Add to the season's total
            seasonVerticalMap.put(seasonID,
                seasonVerticalMap.getOrDefault(seasonID, 0) + vertical);
          } catch (NumberFormatException e) {
            logger.warn("Invalid vertical value: {}", item.get("vertical"));
          }
        }
      }
    }
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

  @GetMapping("/health")
  public String healthCheck() {
    logger.info("Health check endpoint called");
    return "OK";
  }
}