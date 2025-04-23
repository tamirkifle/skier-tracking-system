package skiers.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import skiers.model.SkierVertical;
import skiers.service.SkierService;

@RestController
@RequestMapping("/skiers")
public class SkierReadController {
  
  @Autowired
  private SkierService skierService;

  @GetMapping("/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}")
  public ResponseEntity<?> getSkierDayVertical(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID,
      @PathVariable String skierID) {

    SkierVertical result = skierService.getSkierData(resortID, seasonID, dayID, skierID);
    return ResponseEntity.ok(result);
  }
}
