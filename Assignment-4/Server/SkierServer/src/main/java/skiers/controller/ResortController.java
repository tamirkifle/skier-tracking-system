package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
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
  private final AmazonDynamoDB amazonDynamoDB;

  @Autowired
  public ResortController(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
  }

  @GetMapping("/{resortID}/seasons/{seasonID}/day/{dayID}/skiers")
  public ResponseEntity<?> getUniqueSkiers(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID) {

    // check if URL is valid. Return 400 for invalid URL
    if (!isValidPathParameters(resortID, seasonID, dayID)) {
      logger.warn("Invalid path parameters: resortID={}, seasonID={}, dayID={}", resortID, seasonID, dayID);
      return ResponseEntity.badRequest().body(Map.of("message", "Invalid URL Parameters"));
    }

    try {
      DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDB);
      Table table = dynamoDB.getTable("LiftRideS");

      // scanning for resortID, seasonID and dayID
      ScanFilter resortFilter = new ScanFilter("resortID").eq(resortID);
      ScanFilter seasonFilter = new ScanFilter("seasonID").eq(seasonID);
      ScanFilter dayFilter = new ScanFilter("dayID").eq(dayID);

      ItemCollection<ScanOutcome> items = table.scan(resortFilter, seasonFilter, dayFilter);

      Set<String> uniqueSkiers = new HashSet<>();
      for (Item item : items) {
        String skierID = item.getString("skierID");
        if (skierID != null) {
          uniqueSkiers.add(skierID);
        }
      }

      Map<String, Object> response = new HashMap<>();
      response.put("resortID", resortID);
      response.put("numSkiers", uniqueSkiers.size());
      // 200 response
      return ResponseEntity.ok(response);
    } catch (Exception e) {
      logger.error("Error retrieving data from DynamoDB", e);
      // return 404 if resortID not found
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(Map.of("message", "Resort not found"));
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
