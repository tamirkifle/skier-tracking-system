package skiers.exception;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import skiers.controller.SkierController.ApiError;

/** A client mistake must not be reported as a 500: it inflates the 5xx availability SLI. */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {

    Map<String, Object> fields = new LinkedHashMap<>();
    ex.getBindingResult()
        .getFieldErrors()
        .forEach(error -> fields.put(error.getField(), error.getDefaultMessage()));

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("message", "Invalid LiftRide data");
    body.put("fields", fields);
    return ResponseEntity.badRequest().body(body);
  }

  @ExceptionHandler(CallNotPermittedException.class)
  public ResponseEntity<ApiError> handleCircuitOpen(CallNotPermittedException ex) {
    logger.warn("Circuit breaker '{}' rejected a call", ex.getCausingCircuitBreakerName());
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, "5")
        .body(new ApiError("Datastore temporarily unavailable", "circuit breaker open"));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<ApiError> handleIllegalArgument(IllegalArgumentException ex) {
    return ResponseEntity.badRequest().body(new ApiError("Invalid request", ex.getMessage()));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> handleAllExceptions(Exception ex) {
    String errorId = UUID.randomUUID().toString();
    logger.error("Unhandled failure [{}]", errorId, ex);
    return ResponseEntity.internalServerError()
        .body(new ApiError("Internal server error", "errorId=" + errorId));
  }
}
