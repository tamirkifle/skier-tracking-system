package skiers.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import skiers.Constants;
import skiers.config.SkierProperties;
import skiers.exception.GlobalExceptionHandler;
import skiers.ingest.LiftRidePublisher;
import skiers.ingest.PublishOutcome;
import skiers.metrics.IngestMetrics;
import skiers.model.LiftRide;
import skiers.service.RateLimiter;
import skiers.service.SkierService;

@WebMvcTest(
    controllers = {SkierController.class, SkierReadController.class, ResortController.class})
@Import({GlobalExceptionHandler.class, SkierControllerTest.TestBeans.class})
class SkierControllerTest {

  private static final String INGEST_PATH = "/skiers/5/seasons/2025/days/1/skier/42";

  @TestConfiguration
  static class TestBeans {
    @Bean
    IngestMetrics ingestMetrics() {
      return new IngestMetrics(new SimpleMeterRegistry());
    }

    @Bean
    SkierProperties skierProperties() {
      return new SkierProperties();
    }
  }

  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper objectMapper;

  @MockBean private LiftRidePublisher publisher;
  @MockBean private RateLimiter rateLimiter;
  @MockBean private SkierService skierService;

  private String body(Integer liftId, Integer time) throws Exception {
    return objectMapper.writeValueAsString(new LiftRide(liftId, time));
  }

  private void admitAll() throws InterruptedException {
    when(rateLimiter.tryAcquire(anyLong())).thenReturn(true);
    when(rateLimiter.getCurrentRate()).thenReturn(1700);
    when(publisher.publish(any(), any(), anyLong())).thenReturn(PublishOutcome.CONFIRMED);
  }

  @Test
  @DisplayName("a confirmed publish is answered 201 with its event id")
  void publishesValidEvent() throws Exception {
    admitAll();

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 217)))
        .andExpect(status().isCreated())
        .andExpect(header().exists(Constants.HEADER_EVENT_ID));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, Object>> payload = ArgumentCaptor.forClass(Map.class);
    verify(publisher).publish(any(), payload.capture(), anyLong());

    assertThat(payload.getValue())
        .containsEntry("resortID", "5")
        .containsEntry("seasonID", "2025")
        .containsEntry("dayID", "1")
        .containsEntry("skierID", "42")
        .containsEntry("liftID", 21)
        .containsEntry("time", 217);
  }

  @Test
  @DisplayName("a client-supplied x-event-id is the identity the event is published under")
  void honoursClientEventId() throws Exception {
    admitAll();

    mockMvc
        .perform(
            post(INGEST_PATH)
                .header(Constants.HEADER_EVENT_ID, "ride-7f2c")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isCreated())
        .andExpect(header().string(Constants.HEADER_EVENT_ID, "ride-7f2c"));

    verify(publisher).publish(eq("ride-7f2c"), any(), anyLong());
  }

  @Test
  @DisplayName("an x-event-id outside the accepted shape is a 400, not a header echoed into logs")
  void rejectsMalformedClientEventId() throws Exception {
    admitAll();

    mockMvc
        .perform(
            post(INGEST_PATH)
                .header(Constants.HEADER_EVENT_ID, "ride 7f2c\nX-Injected: yes")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Invalid x-event-id"));

    mockMvc
        .perform(
            post(INGEST_PATH)
                .header(Constants.HEADER_EVENT_ID, "x".repeat(65))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest());

    verify(publisher, never()).publish(any(), any(), anyLong());
  }

  @Test
  @DisplayName("the publish time handed to the publisher is sampled in the handler")
  void samplesPublishTimestampInTheHandler() throws Exception {
    admitAll();

    long before = System.currentTimeMillis();
    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 217)))
        .andExpect(status().isCreated());
    long after = System.currentTimeMillis();

    ArgumentCaptor<Long> publishedAt = ArgumentCaptor.forClass(Long.class);
    verify(publisher).publish(any(), any(), publishedAt.capture());

    // Epoch milliseconds, not the AMQP timestamp property, which the wire rounds to seconds.
    assertThat(publishedAt.getValue()).isBetween(before, after);
  }

  @Test
  @DisplayName("no permit available in time yields 429 with Retry-After and no publish")
  void shedsWhenAdmissionDenied() throws Exception {
    when(rateLimiter.tryAcquire(anyLong())).thenReturn(false);
    when(rateLimiter.getCurrentRate()).thenReturn(400);

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 217)))
        .andExpect(status().isTooManyRequests())
        .andExpect(header().string("Retry-After", "1"))
        .andExpect(jsonPath("$.message").value(Constants.RATE_LIMIT_EXCEEDED));

    verify(publisher, never()).publish(any(), any(), anyLong());
  }

  @Test
  @DisplayName("a refused or returned publish yields 503, never a 2xx the pipeline cannot honour")
  void reportsPublishFailure() throws Exception {
    admitAll();
    when(publisher.publish(any(), any(), anyLong())).thenReturn(PublishOutcome.REJECTED);

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 217)))
        .andExpect(status().isServiceUnavailable())
        .andExpect(header().string("Retry-After", "1"));
  }

  @Test
  @DisplayName("an unconfirmed publish is 504 with the event id, not 503 and not 201")
  void reportsUnknownOutcomeSeparately() throws Exception {
    admitAll();
    when(publisher.publish(any(), any(), anyLong())).thenReturn(PublishOutcome.UNKNOWN);

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 217)))
        .andExpect(status().isGatewayTimeout())
        .andExpect(header().exists(Constants.HEADER_EVENT_ID))
        .andExpect(jsonPath("$.message").value("Event outcome unknown"));
  }

  @Test
  @DisplayName("body outside the documented domain is rejected with the offending fields named")
  void rejectsOutOfRangeBody() throws Exception {
    admitAll();

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(999, 217)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.fields.liftID").exists());

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content(body(21, 0)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.fields.time").exists());

    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content("{}"))
        .andExpect(status().isBadRequest());

    verify(publisher, never()).publish(any(), any(), anyLong());
  }

  @Test
  @DisplayName("an out-of-domain dayID is rejected rather than stored unreachably")
  void rejectsUnreachableDay() throws Exception {
    admitAll();

    mockMvc
        .perform(
            post("/skiers/5/seasons/2025/days/9999/skier/42")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(Constants.INVALID_PATH_PARAMETERS));
  }

  @Test
  @DisplayName("out-of-domain path parameters are rejected before a permit is spent")
  void rejectsBadPathBeforeAdmission() throws Exception {
    mockMvc
        .perform(
            post("/skiers/99/seasons/2025/days/1/skier/42")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest());

    mockMvc
        .perform(
            post("/skiers/5/seasons/1999/days/1/skier/42")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest());

    mockMvc
        .perform(
            post("/skiers/5/seasons/2025/days/1/skier/999999")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(21, 217)))
        .andExpect(status().isBadRequest());

    verify(rateLimiter, never()).tryAcquire(anyLong());
  }

  @Test
  @DisplayName("an unparseable body is a 400, not a 500")
  void unparseableBodyIsNotAServerError() throws Exception {
    admitAll();
    mockMvc
        .perform(post(INGEST_PATH).contentType(MediaType.APPLICATION_JSON).content("{not json"))
        .andExpect(status().isBadRequest());
  }

  @Test
  @DisplayName("read endpoints validate their path parameters")
  void readEndpointsValidate() throws Exception {
    mockMvc
        .perform(get("/skiers/5/seasons/2025/days/1/skiers/notanumber"))
        .andExpect(status().isBadRequest());

    mockMvc
        .perform(get("/resorts/99/seasons/2025/day/1/skiers"))
        .andExpect(status().isBadRequest());
  }

  @Test
  @DisplayName("a skier with no recorded vertical is a 404, not an empty 200")
  void emptyResortTotalsIs404() throws Exception {
    when(skierService.getSkierResortTotals("42", "5", null))
        .thenReturn(Map.of("resorts", java.util.List.of()));

    mockMvc
        .perform(get("/skiers/42/vertical").param("resort", "5"))
        .andExpect(status().isNotFound());
  }
}
