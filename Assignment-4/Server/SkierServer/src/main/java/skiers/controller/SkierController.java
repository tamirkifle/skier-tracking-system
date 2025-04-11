package skiers.controller;

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

  @GetMapping("/health")
  public String healthCheck() {
    logger.info("Health check endpoint called");
    return "OK";
  }
}