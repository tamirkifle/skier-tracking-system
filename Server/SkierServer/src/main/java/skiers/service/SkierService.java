package skiers.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import skiers.Constants;
import skiers.model.ResortSkierCount;
import skiers.model.SkierVertical;

@Service
public class SkierService {
  
  private static final Logger logger = LoggerFactory.getLogger(SkierService.class);
  
  private final AmazonDynamoDB amazonDynamoDB;

  @Autowired
  public SkierService(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
  }

  /**
   * Get skier day vertical data with caching
   */
  @Cacheable(value = "skierDayVertical", key = "{#resortID, #seasonID, #dayID, #skierID}")
  public SkierVertical getSkierData(String resortID, String seasonID, String dayID, String skierID) {
    int verticalTotal = fetchVerticalFromDB(resortID, seasonID, dayID, skierID);
    return new SkierVertical(resortID, seasonID, dayID, skierID, verticalTotal);
  }

  /**
   * Get unique skiers at a resort for a season/day with caching
   */
  @Cacheable(value = "resortSkierCount", key = "{#resortID, #seasonID, #dayID}")
  public ResortSkierCount getUniqueSkiersCount(String resortID, String seasonID, String dayID) {
    int count = fetchUniqueSkiersCount(resortID, seasonID, dayID);
    return new ResortSkierCount(resortID, count);
  }

  /**
   * Get skier resort totals with optional season filter, with caching
   */
  @Cacheable(value = "skierResortTotals", key = "{#skierID, #resort, #season}")
  public Map<String, Object> getSkierResortTotals(String skierID, String resort, String season) {
    Map<String, Integer> seasonVerticalMap = fetchSkierResortTotals(skierID, resort, season);
    
    // Format response
    Map<String, Object> response = new HashMap<>();
    List<Map<String, Object>> resorts = new ArrayList<>();

    // Add all seasons to the response
    for (Map.Entry<String, Integer> entry : seasonVerticalMap.entrySet()) {
      Map<String, Object> seasonData = new HashMap<>();
      seasonData.put("seasonID", entry.getKey());
      seasonData.put("totalVert", entry.getValue());
      resorts.add(seasonData);
    }

    response.put("resorts", resorts);
    return response;
  }

  /**
   * Internal method to fetch vertical data from database
   */
  private int fetchVerticalFromDB(String resortID, String seasonID, String dayID, String skierID) {
    try {
      // Create the composite key prefix
      String compositeKeyPrefix = resortID + "#" + seasonID + "#" + dayID + "#";

      // Set up query parameters
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":skierId", new AttributeValue().withS(skierID));
      expressionValues.put(":compositeKeyPrefix", new AttributeValue().withS(compositeKeyPrefix));

      // Use attribute name from the table
      Map<String, String> expressionNames = new HashMap<>();
      expressionNames.put("#compositeKey", "resortID#seasonID#dayID#timestamp");

      // Build the query
      QueryRequest queryRequest = new QueryRequest()
          .withTableName(Constants.TARGET_TABLE_NAME)
          .withKeyConditionExpression("skierID = :skierId AND begins_with(#compositeKey, :compositeKeyPrefix)")
          .withExpressionAttributeNames(expressionNames)
          .withExpressionAttributeValues(expressionValues);

      QueryResult result = amazonDynamoDB.query(queryRequest);

      int verticalTotal = result.getItems().stream()
          .mapToInt(item -> {
            AttributeValue vertical = item.get("vertical");
            if (vertical == null) return 0;
            return vertical.getN() != null ?
                Integer.parseInt(vertical.getN()) :
                Integer.parseInt(vertical.getS());
          })
          .sum();

      return verticalTotal;
    } catch (Exception e) {
      logger.error("Error querying vertical data from DynamoDB", e);
      throw new RuntimeException("Database error: " + e.getMessage(), e);
    }
  }

  /**
   * Internal method to fetch unique skiers count from database
   */
  private int fetchUniqueSkiersCount(String resortID, String seasonID, String dayID) {
    try {
      String key = resortID + "#" + seasonID + "#" + dayID;

      Map<String, AttributeValue> keyToGet = new HashMap<>();
      keyToGet.put("resortSeasonDay", new AttributeValue().withS(key));

      Map<String, AttributeValue> item = amazonDynamoDB.getItem("SkierCounts", keyToGet).getItem();

      int count = 0;
      if (item != null && item.containsKey("uniqueSkierCount")) {
        count = Integer.parseInt(item.get("uniqueSkierCount").getN());
      }

      return count;
    } catch (Exception e) {
      logger.error("Error fetching unique skiers count from DynamoDB", e);
      throw new RuntimeException("Database error: " + e.getMessage(), e);
    }
  }

  /**
   * Internal method to fetch skier resort totals from database
   */
  private Map<String, Integer> fetchSkierResortTotals(String skierID, String resort, String season) {
    Map<String, Integer> seasonVerticalMap = new HashMap<>();
    
    try {
      if (season != null && !season.isEmpty()) {
        // When season is specified, use the CS-Index
        String partitionKey = resort + "#" + skierID;

        // Build expression attribute values
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":partitionKey", new AttributeValue().withS(partitionKey));
        expressionAttributeValues.put(":seasonPrefix", new AttributeValue().withS(season + "#"));

        // Add expression attribute names to handle the "#" character
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#partitionKeyAttr", "resortID#skierID");
        expressionAttributeNames.put("#sortKeyAttr", "seasonID#dayID");

        // Query against CS-Index with begins_with on the sort key
        QueryRequest queryRequest = new QueryRequest()
            .withTableName(Constants.TARGET_TABLE_NAME)
            .withIndexName("CS-Index")
            .withKeyConditionExpression("#partitionKeyAttr = :partitionKey AND begins_with(#sortKeyAttr, :seasonPrefix)")
            .withExpressionAttributeValues(expressionAttributeValues)
            .withExpressionAttributeNames(expressionAttributeNames);

        // Execute and process
        int totalVertical = executeVerticalQuery(queryRequest);

        // Add to seasonVerticalMap if results found
        if (totalVertical > 0) {
          seasonVerticalMap.put(season, totalVertical);
        }

      } else {
        // When no season is specified, still use CS-Index but don't filter by season
        String partitionKey = resort + "#" + skierID;

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":partitionKey", new AttributeValue().withS(partitionKey));

        // Add expression attribute names to handle the "#" character
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#partitionKeyAttr", "resortID#skierID");

        // Query against CS-Index with proper attribute naming
        QueryRequest queryRequest = new QueryRequest()
            .withTableName(Constants.TARGET_TABLE_NAME)
            .withIndexName("CS-Index")
            .withKeyConditionExpression("#partitionKeyAttr = :partitionKey")
            .withExpressionAttributeValues(expressionAttributeValues)
            .withExpressionAttributeNames(expressionAttributeNames);

        // Execute query
        QueryResult queryResult = amazonDynamoDB.query(queryRequest);

        // Group and sum by season
        processVerticalBySeasons(queryResult.getItems(), seasonVerticalMap);

        // Handle pagination
        while (queryResult.getLastEvaluatedKey() != null && !queryResult.getLastEvaluatedKey().isEmpty()) {
          queryRequest.withExclusiveStartKey(queryResult.getLastEvaluatedKey());
          queryResult = amazonDynamoDB.query(queryRequest);
          processVerticalBySeasons(queryResult.getItems(), seasonVerticalMap);
        }
      }
      
    } catch (Exception e) {
      logger.error("Error fetching skier resort totals from DynamoDB", e);
      throw new RuntimeException("Database error: " + e.getMessage(), e);
    }
    
    return seasonVerticalMap;
  }

  /**
   * Helper method to execute query and sum vertical
   */
  private int executeVerticalQuery(QueryRequest queryRequest) {
    int totalVertical = 0;
    try {
      QueryResult queryResult = amazonDynamoDB.query(queryRequest);

      // Process initial results
      for (Map<String, AttributeValue> item : queryResult.getItems()) {
        if (item.containsKey("vertical")) {
          try {
            totalVertical += Integer.parseInt(item.get("vertical").getS());
          } catch (NumberFormatException e) {
            logger.warn("Invalid vertical value: {}", item.get("vertical").getS());
          }
        }
      }

      // Handle pagination
      while (queryResult.getLastEvaluatedKey() != null && !queryResult.getLastEvaluatedKey().isEmpty()) {
        queryRequest.withExclusiveStartKey(queryResult.getLastEvaluatedKey());
        queryResult = amazonDynamoDB.query(queryRequest);

        for (Map<String, AttributeValue> item : queryResult.getItems()) {
          if (item.containsKey("vertical")) {
            try {
              totalVertical += Integer.parseInt(item.get("vertical").getS());
            } catch (NumberFormatException e) {
              logger.warn("Invalid vertical value: {}", item.get("vertical").getS());
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error executing vertical query: {}", e.getMessage(), e);
    }
    return totalVertical;
  }

  /**
   * Helper method to process verticals by season
   */
  private void processVerticalBySeasons(List<Map<String, AttributeValue>> items, Map<String, Integer> seasonVerticalMap) {
    for (Map<String, AttributeValue> item : items) {
      // Extract season from the sort key (seasonID#dayID)
      if (item.containsKey("seasonID#dayID") && item.containsKey("vertical")) {
        String sortKey = item.get("seasonID#dayID").getS();
        String seasonID = sortKey.split("#")[0]; // Extract season from sort key

        try {
          int vertical = Integer.parseInt(item.get("vertical").getS());
          // Update season total
          seasonVerticalMap.put(seasonID, seasonVerticalMap.getOrDefault(seasonID, 0) + vertical);
        } catch (NumberFormatException e) {
          logger.warn("Invalid vertical value: {}", item.get("vertical").getS());
        }
      }
    }
  }
}
