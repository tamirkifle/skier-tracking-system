package skiers.controller;

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
import skiers.model.ResortSkierCount;
import skiers.service.SkierService;

import java.util.Map;

@RestController
@RequestMapping("/resorts")
public class ResortController {

  private static final Logger logger = LoggerFactory.getLogger(ResortController.class);
  
  @Autowired
  private SkierService skierService;

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
      // Get data from service layer with caching
      ResortSkierCount result = skierService.getUniqueSkiersCount(resortID, seasonID, dayID);
      return ResponseEntity.ok(result);
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
