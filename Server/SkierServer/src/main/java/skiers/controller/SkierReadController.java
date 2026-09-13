package skiers.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import skiers.Constants;
import skiers.model.SkierVertical;
import skiers.service.SkierService;

/** Read path: a single skier's vertical for a single day. */
@RestController
@RequestMapping("/skiers")
@Tag(name = "Skiers", description = "Lift-ride ingestion and per-skier queries")
public class SkierReadController {

  private final SkierService skierService;

  public SkierReadController(SkierService skierService) {
    this.skierService = skierService;
  }

  @GetMapping("/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}")
  @Operation(
      summary = "Vertical for one skier on one day",
      description = "Single bounded range read on the base table's sort key; no index required.")
  public ResponseEntity<?> getSkierDayVertical(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID,
      @PathVariable String skierID) {

    if (!isValid(resortID, seasonID, dayID, skierID)) {
      return ResponseEntity.badRequest()
          .body(
              new SkierController.ApiError(
                  Constants.INVALID_PATH_PARAMETERS, "resortID/seasonID/dayID/skierID"));
    }

    SkierVertical result = skierService.getSkierData(resortID, seasonID, dayID, skierID);
    return ResponseEntity.ok(result);
  }

  private boolean isValid(String resortID, String seasonID, String dayID, String skierID) {
    try {
      int resort = Integer.parseInt(resortID);
      int season = Integer.parseInt(seasonID);
      int day = Integer.parseInt(dayID);
      int skier = Integer.parseInt(skierID);

      return resort >= Constants.MIN_RESORT_ID
          && resort <= Constants.MAX_RESORT_ID
          && season == Constants.SEASON_ID
          && day == Constants.DAY_ID
          && skier >= Constants.MIN_SKIER_ID
          && skier <= Constants.MAX_SKIER_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
