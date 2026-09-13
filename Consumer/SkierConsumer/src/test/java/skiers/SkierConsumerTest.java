package skiers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import skiers.ingest.DeliveryAck;
import skiers.model.LiftRideEvent;

class SkierConsumerTest {

  private static final Map<String, Object> BODY =
      Map.of(
          "skierID", "42",
          "resortID", "5",
          "seasonID", "2025",
          "dayID", "1",
          "liftID", 21,
          "time", 217);

  private static Message messageWith(String header, Object value) {
    MessageProperties properties = new MessageProperties();
    properties.setHeader(Constants.HEADER_EVENT_ID, "evt-1");
    if (header != null) {
      properties.setHeader(header, value);
    }
    return new Message(new byte[0], properties);
  }

  @Test
  @DisplayName("the publish timestamp is carried through to the event")
  void readsPublishTimestampHeader() {
    LiftRideEvent event =
        SkierConsumer.parse(
            BODY, messageWith(Constants.HEADER_PUBLISHED_AT, 1_724_000_000_123L), DeliveryAck.NONE);

    assertThat(event.publishedAtMillis()).isEqualTo(1_724_000_000_123L);
  }

  @Test
  @DisplayName("a timestamp that arrives as a string is still usable")
  void acceptsPublishTimestampAsText() {
    LiftRideEvent event =
        SkierConsumer.parse(
            BODY, messageWith(Constants.HEADER_PUBLISHED_AT, "1724000000123"), DeliveryAck.NONE);

    assertThat(event.publishedAtMillis()).isEqualTo(1_724_000_000_123L);
  }

  @Test
  @DisplayName("a message with no publish timestamp parses, and reports none")
  void toleratesMissingPublishTimestamp() {
    LiftRideEvent event = SkierConsumer.parse(BODY, messageWith(null, null), DeliveryAck.NONE);

    // A zero would be recorded as decades of pipeline latency.
    assertThat(event.publishedAtMillis()).isNull();
  }

  @Test
  @DisplayName("an unparseable publish timestamp is dropped, not allowed to fail the delivery")
  void ignoresUnparseablePublishTimestamp() {
    LiftRideEvent event =
        SkierConsumer.parse(
            BODY, messageWith(Constants.HEADER_PUBLISHED_AT, "not-a-timestamp"), DeliveryAck.NONE);

    assertThat(event.publishedAtMillis()).isNull();
    assertThat(event.skierId()).isEqualTo("42");
  }

  @Test
  @DisplayName("the attempt count is read from the delivery, not restarted at zero")
  void readsAttemptCountFromTheDelivery() {
    LiftRideEvent event =
        SkierConsumer.parse(BODY, messageWith(Constants.HEADER_ATTEMPTS, 3), DeliveryAck.NONE);

    assertThat(event.attempts()).isEqualTo(3);
    assertThat(event.recordAttempt()).isEqualTo(4);
  }

  @Test
  @DisplayName("an attempt count sent as text is still counted")
  void acceptsAttemptCountAsText() {
    LiftRideEvent event =
        SkierConsumer.parse(BODY, messageWith(Constants.HEADER_ATTEMPTS, "2"), DeliveryAck.NONE);

    assertThat(event.attempts()).isEqualTo(2);
  }

  @Test
  @DisplayName("a first delivery, or one with an unreadable count, starts at zero")
  void defaultsAttemptCountToZero() {
    assertThat(SkierConsumer.parse(BODY, messageWith(null, null), DeliveryAck.NONE).attempts())
        .isZero();

    LiftRideEvent malformed =
        SkierConsumer.parse(BODY, messageWith(Constants.HEADER_ATTEMPTS, "many"), DeliveryAck.NONE);
    assertThat(malformed.attempts()).isZero();
    assertThat(malformed.skierId()).isEqualTo("42");
  }

  @Test
  @DisplayName("a missing payload field is still fatal")
  void stillRejectsAMalformedBody() {
    assertThatThrownBy(
            () ->
                SkierConsumer.parse(
                    Map.of("skierID", "42"), messageWith(null, null), DeliveryAck.NONE))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
