package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import skiers.Constants;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/skiers")
public class SkierReadController {
  private final AmazonDynamoDB amazonDynamoDB;

  @Autowired
  public SkierReadController(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
  }

  @GetMapping("/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}")
  public ResponseEntity<?> getSkierDayVertical(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID,
      @PathVariable String skierID) {

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

      return ResponseEntity.ok(Map.of(
          "resortID", resortID,
          "seasonID", seasonID,
          "dayID", dayID,
          "skierID", skierID,
          "totalVertical", verticalTotal
      ));

    } catch (Exception e) {
      return ResponseEntity.internalServerError()
          .body(Map.of("error", "Database error", "message", e.getMessage()));
    }
  }
}
