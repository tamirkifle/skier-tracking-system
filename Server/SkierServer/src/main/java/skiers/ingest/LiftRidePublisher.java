package skiers.ingest;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import skiers.Constants;
import skiers.config.SkierProperties;

/**
 * Publishes a lift ride and waits for the broker to confirm it. {@code convertAndSend} returns once
 * the frames are written to the channel, which is earlier than the broker taking ownership, so the
 * 201 waits on the confirm rather than on the socket.
 */
@Component
public class LiftRidePublisher {

  private static final Logger logger = LoggerFactory.getLogger(LiftRidePublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final long confirmTimeoutMs;

  public LiftRidePublisher(
      RabbitTemplate rabbitTemplate, ConnectionFactory connectionFactory, SkierProperties props) {
    this.rabbitTemplate = rabbitTemplate;
    this.confirmTimeoutMs = props.getIngest().getConfirmTimeoutMs();

    // Only correlated confirms complete the future below, so without them every publish would
    // time out into UNKNOWN and the endpoint would stop answering 201.
    if (!connectionFactory.isPublisherConfirms()) {
      throw new IllegalStateException(
          "spring.rabbitmq.publisher-confirm-type must be 'correlated': the ingest endpoint's 201 "
              + "means the broker confirmed the publish, which cannot be established without it");
    }
    if (!connectionFactory.isPublisherReturns()) {
      throw new IllegalStateException(
          "spring.rabbitmq.publisher-returns must be true: without it an unroutable publish is "
              + "acked and silently discarded, and this service would report it as accepted");
    }
  }

  public PublishOutcome publish(String eventId, Map<String, Object> body, long publishedAtMillis) {
    CorrelationData correlation = new CorrelationData(eventId);

    try {
      rabbitTemplate.convertAndSend(
          Constants.LIFT_RIDE_EXCHANGE,
          Constants.LIFT_RIDE_ROUTING_KEY,
          body,
          withEnvelope(eventId, publishedAtMillis),
          correlation);
    } catch (AmqpException e) {
      logger.error("Publish of event {} failed outright: {}", eventId, e.getMessage());
      return PublishOutcome.REJECTED;
    }

    // A timeout is UNKNOWN rather than a failure: the publish may be confirmed a moment later,
    // and reporting "not accepted" invites a retry that duplicates the event.
    CorrelationData.Confirm confirm;
    try {
      confirm = correlation.getFuture().get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      logger.warn("No publish confirm for event {} within {}ms", eventId, confirmTimeoutMs);
      return PublishOutcome.UNKNOWN;
    } catch (ExecutionException e) {
      logger.warn("Publish confirm for event {} failed: {}", eventId, e.getMessage());
      return PublishOutcome.UNKNOWN;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return PublishOutcome.UNKNOWN;
    }

    // An unroutable mandatory publish is returned and then acked, so the confirm alone reports a
    // discarded message as accepted. The return is checked first.
    if (correlation.getReturned() != null) {
      logger.error(
          "Event {} was returned as unroutable: exchange={} routingKey={} replyText={}",
          eventId,
          correlation.getReturned().getExchange(),
          correlation.getReturned().getRoutingKey(),
          correlation.getReturned().getReplyText());
      return PublishOutcome.REJECTED;
    }
    if (confirm == null || !confirm.isAck()) {
      logger.error(
          "Broker nacked event {}: {}",
          eventId,
          confirm == null ? "no confirm" : confirm.getReason());
      return PublishOutcome.REJECTED;
    }
    return PublishOutcome.CONFIRMED;
  }

  /**
   * Marks the message persistent, since a durable queue holding transient messages loses them on
   * broker restart. The publish instant is sampled by the caller so it matches the ingest timer.
   */
  private static MessagePostProcessor withEnvelope(String eventId, long publishedAtMillis) {
    return message -> {
      message.getMessageProperties().setHeader(Constants.HEADER_EVENT_ID, eventId);
      message.getMessageProperties().setHeader(Constants.HEADER_PUBLISHED_AT, publishedAtMillis);
      message.getMessageProperties().setMessageId(eventId);
      message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
      return message;
    };
  }
}
