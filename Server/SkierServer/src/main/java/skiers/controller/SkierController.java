package skiers.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.ingest.LiftRidePublisher;
import skiers.ingest.PublishOutcome;
import skiers.metrics.IngestMetrics;
import skiers.model.LiftRide;
import skiers.service.RateLimiter;
import skiers.service.SkierService;

/** Write path: accepts lift-ride events and hands them to the broker. */
@RestController
@RequestMapping("/skiers")
@Tag(name = "Skiers", description = "Lift-ride ingestion and per-skier queries")
public class SkierController {

  private static final Logger logger = LoggerFactory.getLogger(SkierController.class);

  private final LiftRidePublisher publisher;
  private final RateLimiter rateLimiter;
  private final SkierService skierService;
  private final IngestMetrics metrics;
  private final SkierProperties.Admission admissionConfig;
  private final SkierProperties.Ingest ingestConfig;

  public SkierController(
      LiftRidePublisher publisher,
      RateLimiter rateLimiter,
      SkierService skierService,
      IngestMetrics metrics,
      SkierProperties properties) {
    this.publisher = publisher;
    this.rateLimiter = rateLimiter;
    this.skierService = skierService;
    this.metrics = metrics;
    this.admissionConfig = properties.getAdmission();
    this.ingestConfig = properties.getIngest();
  }

  @PostMapping("/{resortID}/seasons/{seasonID}/days/{dayID}/skier/{skierID}")
  @Operation(
      summary = "Record a lift ride",
      description =
          "Publishes the event for asynchronous persistence and waits for the broker's publisher "
              + "confirm. A 201 means the broker confirmed the publish and did not return it as "
              + "unroutable; it does not mean the event is yet visible to the read APIs. Supply "
              + "an x-event-id header to retry a request whose outcome you did not learn: the same "
              + "id is idempotent through the whole write path.")
  @ApiResponses({
    @ApiResponse(responseCode = "201", description = "Publish confirmed by the broker"),
    @ApiResponse(
        responseCode = "400",
        description = "Path parameters, body or x-event-id outside the valid domain",
        content = @Content(schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(
        responseCode = "429",
        description = "Admission control shed the request; retry after the indicated delay",
        content = @Content(schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(
        responseCode = "503",
        description = "The broker refused or returned the publish; the event was not accepted",
        content = @Content(schema = @Schema(implementation = ApiError.class))),
    @ApiResponse(
        responseCode = "504",
        description =
            "No publish confirm arrived in time; the outcome is unknown. Retry with the returned "
                + "x-event-id rather than treating this as a failure",
        content = @Content(schema = @Schema(implementation = ApiError.class)))
  })
  public ResponseEntity<?> addLiftRide(
      @PathVariable String resortID,
      @PathVariable String seasonID,
      @PathVariable String dayID,
      @PathVariable String skierID,
      @RequestHeader(name = Constants.HEADER_EVENT_ID, required = false) String clientEventId,
      @Valid @RequestBody LiftRide liftRide)
      throws InterruptedException {

    if (clientEventId != null && !isValidEventId(clientEventId)) {
      metrics.recordInvalid();
      return ResponseEntity.badRequest()
          .body(
              ApiError.of(
                  "Invalid x-event-id",
                  "expected up to "
                      + ingestConfig.getMaxEventIdLength()
                      + " characters of [A-Za-z0-9._:-]"));
    }

    if (!isValidPathParameters(resortID, seasonID, dayID, skierID)) {
      metrics.recordInvalid();
      logger.debug(
          "Rejecting out-of-domain path: resort={} season={} day={} skier={}",
          resortID,
          seasonID,
          dayID,
          skierID);
      return ResponseEntity.badRequest()
          .body(ApiError.of(Constants.INVALID_PATH_PARAMETERS, "resortID/seasonID/dayID/skierID"));
    }

    long admissionStart = System.nanoTime();
    boolean admitted = rateLimiter.tryAcquire(admissionConfig.getMaxWaitMs());
    long admissionWait = System.nanoTime() - admissionStart;

    if (!admitted) {
      metrics.recordShed(admissionWait);
      return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
          .header(HttpHeaders.RETRY_AFTER, "1")
          .body(
              ApiError.of(
                  Constants.RATE_LIMIT_EXCEEDED,
                  "admission rate is " + rateLimiter.getCurrentRate() + " events/s"));
    }
    metrics.recordAdmissionWait(admissionWait);

    String eventId =
        clientEventId == null || clientEventId.isBlank()
            ? UUID.randomUUID().toString()
            : clientEventId;
    Map<String, Object> message =
        Map.of(
            "resortID", resortID,
            "seasonID", seasonID,
            "dayID", dayID,
            "skierID", skierID,
            "liftID", liftRide.getLiftID(),
            "time", liftRide.getTime());

    long publishStart = System.nanoTime();
    PublishOutcome outcome = publisher.publish(eventId, message, System.currentTimeMillis());
    long publishNanos = System.nanoTime() - publishStart;

    switch (outcome) {
      case CONFIRMED -> {
        metrics.recordAccepted(publishNanos);
        return ResponseEntity.status(HttpStatus.CREATED)
            .header(Constants.HEADER_EVENT_ID, eventId)
            .build();
      }
      case REJECTED -> {
        metrics.recordPublishFailure();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .header(HttpHeaders.RETRY_AFTER, "1")
            .header(Constants.HEADER_EVENT_ID, eventId)
            .body(ApiError.of("Event not accepted", "the broker refused or returned the publish"));
      }
      default -> {
        // 504 rather than 503: a 503 says the event is not in the queue, this says nobody knows.
        // The event id goes back so a retry is the same event rather than a duplicate.
        metrics.recordPublishUnknown();
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
            .header(HttpHeaders.RETRY_AFTER, "1")
            .header(Constants.HEADER_EVENT_ID, eventId)
            .body(
                ApiError.of(
                    "Event outcome unknown",
                    "no publisher confirm within "
                        + ingestConfig.getConfirmTimeoutMs()
                        + "ms; retry with this x-event-id"));
      }
    }
  }

  private boolean isValidEventId(String eventId) {
    if (eventId.isEmpty() || eventId.length() > ingestConfig.getMaxEventIdLength()) {
      return false;
    }
    return eventId.chars().allMatch(SkierController::isEventIdCharacter);
  }

  private static boolean isEventIdCharacter(int c) {
    return (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9')
        || c == '-'
        || c == '_'
        || c == '.'
        || c == ':';
  }

  @GetMapping("/{skierID}/vertical")
  @Operation(
      summary = "Total vertical for a skier at a resort",
      description = "Served from cache when warm; falls back to a CS-Index query.")
  public ResponseEntity<?> getSkierResortTotals(
      @PathVariable String skierID,
      @RequestParam String resort,
      @RequestParam(required = false) String season) {

    if (!isValidSkierResort(skierID, resort)) {
      metrics.recordInvalid();
      return ResponseEntity.badRequest()
          .body(ApiError.of("Invalid input parameters", "skierID/resort"));
    }

    Map<String, Object> response = skierService.getSkierResortTotals(skierID, resort, season);
    List<?> resorts = (List<?>) response.get("resorts");
    if (resorts == null || resorts.isEmpty()) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(ApiError.of("No vertical data found for the specified parameters", null));
    }
    return ResponseEntity.ok(response);
  }

  private boolean isValidPathParameters(
      String resortID, String seasonID, String dayID, String skierID) {
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

  private boolean isValidSkierResort(String skierID, String resort) {
    return skierID != null && !skierID.isBlank() && resort != null && !resort.isBlank();
  }

  @Schema(description = "Error response")
  public record ApiError(String message, String detail) {
    static ApiError of(String message, String detail) {
      return new ApiError(message, detail);
    }
  }
}
