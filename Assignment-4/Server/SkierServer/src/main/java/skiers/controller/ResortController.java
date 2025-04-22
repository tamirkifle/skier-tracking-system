package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import java.util.HashMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import skiers.Constants;


//   /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers:
// summary: get number of unique skiers at resort/season/day
@RestController
@RequestMapping("/resorts")
public class ResortController {

  private static final Logger logger = LoggerFactory.getLogger(ResortController.class);
  @Autowired
  private AmazonDynamoDB amazonDynamoDB;

  @GetMapping("/{resortID}/seasons/{seasonID}/day/{dayID}/skiers")
  public ResponseEntity<?> getUniqueSkiers(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID) {

    // check if URL is valid. Return 400 for invalid URL
    if (!isValidPathParameters(resortID, seasonID, dayID)) {
      logger.warn("Invalid path parameters: resortID={}, seasonID={}, dayID={}", resortID, seasonID,
          dayID);
      return ResponseEntity.badRequest().body(Map.of("message", "Invalid URL Parameters"));
    }

    try {

      String key = resortID + "#" + seasonID + "#" + dayID;

      Map<String, AttributeValue> keyToGet = new HashMap<>();
      keyToGet.put("resortSeasonDay", new AttributeValue().withS(key));

      Map<String, AttributeValue> item = amazonDynamoDB.getItem("SkierCounts", keyToGet).getItem();

      int count = 0;
      if (item != null && item.containsKey("uniqueSkierCount")) {
        count = Integer.parseInt(item.get("uniqueSkierCount").getN());
      }

      Map<String, Object> response = new HashMap<>();
      response.put("resortID", resortID);
      response.put("uniqueNumSkiers", count);

      return ResponseEntity.ok(response);





/**
      String gsiKey = resortID + "#" + seasonID + "#" + dayID;

      Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
      expressionAttributeValues.put(":resortSeasonDay", new AttributeValue().withS(gsiKey));

      QueryRequest queryRequest = new QueryRequest()
          .withTableName(Constants.TARGET_TABLE_NAME)
          .withIndexName("resortSeasonDay-index")
          .withKeyConditionExpression("resortSeasonDay = :resortSeasonDay")
          .withExpressionAttributeValues(expressionAttributeValues);


      Set<String> uniqueSkiers = new HashSet<>();



      /*
      QueryResult queryResult = amazonDynamoDB.query(queryRequest);
      for (Map<String, AttributeValue> item : queryResult.getItems()) {
        AttributeValue skierIdAttr = item.get("skierID");
        if (skierIdAttr != null) {
          uniqueSkiers.add(skierIdAttr.getS());
        }
      }
      if (queryResult.getLastEvaluatedKey() != null) {
        Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();
        exclusiveStartKey.put("skierID", new AttributeValue().withS(queryResult.getLastEvaluatedKey()));
        queryRequest.setExclusiveStartKey(exclusiveStartKey);
      }
      */



/**
      Map<String, AttributeValue> lastEvaluatedKey = null;
      do {
        if (lastEvaluatedKey != null) {
          queryRequest.setExclusiveStartKey(lastEvaluatedKey); // Set the exclusive start key for pagination
        }

        // Execute the query
        QueryResult queryResult = amazonDynamoDB.query(queryRequest);

        // Process the query result and add skier IDs to the set
        for (Map<String, AttributeValue> item : queryResult.getItems()) {
          AttributeValue skierIdAttr = item.get("skierID");
          if (skierIdAttr != null) {
            uniqueSkiers.add(skierIdAttr.getS());
          }
        }

        // Update the lastEvaluatedKey for the next query if more pages are available
        lastEvaluatedKey = queryResult.getLastEvaluatedKey();

      } while (lastEvaluatedKey != null); // Continue querying until no more pages

      Map<String, Object> response = new HashMap<>();
      response.put("resortID", resortID);
      response.put("numSkiers", uniqueSkiers.size());

      return ResponseEntity.ok(response);
      **/




    } catch (Exception e) {
      logger.error("Error querying DynamoDB", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("message", "Internal Server Error: " + e.getMessage()));
    }
  }


// check resortID, seasonID, dayID
  private boolean isValidPathParameters(String resortID, String seasonID, String dayID) {
    try {
      int resort = Integer.parseInt(resortID);
      int season = Integer.parseInt(seasonID);
      int day = Integer.parseInt(dayID);

      return resort >= Constants.MIN_RESORT_ID && resort <= Constants.MAX_RESORT_ID &&
          season == Constants.SEASON_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
