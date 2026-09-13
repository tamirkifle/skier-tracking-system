package skiers.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import skiers.Constants;
import skiers.model.ResortSkierCount;
import skiers.service.SkierService;

/** Read path: resort-level aggregates. */
@RestController
@RequestMapping("/resorts")
@Tag(name = "Resorts", description = "Resort-level aggregate queries")
public class ResortController {

  private static final Logger logger = LoggerFactory.getLogger(ResortController.class);

  private final SkierService skierService;

  public ResortController(SkierService skierService) {
    this.skierService = skierService;
  }

  @GetMapping("/{resortID}/seasons/{seasonID}/day/{dayID}/skiers")
  @Operation(
      summary = "Unique skier count for a resort-day",
      description =
          "Read from a precomputed counter. Reflects events the consumer has already persisted, so it "
              + "trails live ingest by the pipeline's end-to-end lag.")
  public ResponseEntity<?> getUniqueSkiers(
      @PathVariable String resortID, @PathVariable String seasonID, @PathVariable String dayID) {

    if (!isValidPathParameters(resortID, seasonID, dayID)) {
      logger.debug(
          "Rejecting out-of-domain path: resort={} season={} day={} skier={}",
          resortID,
          seasonID,
          dayID);
      return ResponseEntity.badRequest()
          .body(new SkierController.ApiError("Invalid URL Parameters", "resortID/seasonID/dayID"));
    }

    ResortSkierCount result = skierService.getUniqueSkiersCount(resortID, seasonID, dayID);
    return ResponseEntity.ok(result);
  }

  private boolean isValidPathParameters(String resortID, String seasonID, String dayID) {
    try {
      int resort = Integer.parseInt(resortID);
      int season = Integer.parseInt(seasonID);
      int day = Integer.parseInt(dayID);

      return resort >= Constants.MIN_RESORT_ID
          && resort <= Constants.MAX_RESORT_ID
          && season == Constants.SEASON_ID
          && day == Constants.DAY_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
