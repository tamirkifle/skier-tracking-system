package skiers;

import com.rabbitmq.client.Channel;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;
import skiers.ingest.AmqpDeliveryAck;
import skiers.ingest.DeliveryAck;
import skiers.ingest.RetryRouter;
import skiers.model.LiftRideEvent;

@Service
public class SkierConsumer {

  private static final Logger logger = LoggerFactory.getLogger(SkierConsumer.class);

  private final SkierDataProcessor dataProcessor;
  private final RetryRouter retryRouter;

  public SkierConsumer(SkierDataProcessor dataProcessor, RetryRouter retryRouter) {
    this.dataProcessor = dataProcessor;
    this.retryRouter = retryRouter;
  }

  @RabbitListener(
      queues = Constants.MAIN_QUEUE,
      containerFactory = "rabbitListenerContainerFactory",
      ackMode = "MANUAL")
  public void processMessage(Map<String, Object> message, Message amqpMessage, Channel channel) {
    DeliveryAck ack =
        new AmqpDeliveryAck(
            channel, amqpMessage.getMessageProperties().getDeliveryTag(), amqpMessage, retryRouter);

    LiftRideEvent event;
    try {
      event = parse(message, amqpMessage, ack);
    } catch (RuntimeException e) {
      // A malformed message never becomes valid, so it is dead-lettered rather than requeued.
      logger.error("Rejecting unparseable message {}: {}", message, e.getMessage());
      ack.reject(false);
      return;
    }

    dataProcessor.submit(event);
  }

  static LiftRideEvent parse(Map<String, Object> message, Message amqpMessage, DeliveryAck ack) {
    Object eventIdHeader = amqpMessage.getMessageProperties().getHeader(Constants.HEADER_EVENT_ID);
    return new LiftRideEvent(
        eventIdHeader == null ? null : eventIdHeader.toString(),
        requireString(message, "skierID"),
        requireString(message, "resortID"),
        requireString(message, "seasonID"),
        requireString(message, "dayID"),
        requireInt(message, "liftID"),
        requireInt(message, "time"),
        ack,
        optionalEpochMillis(amqpMessage, Constants.HEADER_PUBLISHED_AT),
        priorAttempts(amqpMessage));
  }

  private static int priorAttempts(Message amqpMessage) {
    Object value = amqpMessage.getMessageProperties().getHeader(Constants.HEADER_ATTEMPTS);
    if (value instanceof Number number) {
      return Math.max(0, number.intValue());
    }
    if (value instanceof String text) {
      try {
        return Math.max(0, Integer.parseInt(text.trim()));
      } catch (NumberFormatException e) {
        logger.debug("Ignoring unparseable {} header: {}", Constants.HEADER_ATTEMPTS, text);
      }
    }
    return 0;
  }

  private static Long optionalEpochMillis(Message amqpMessage, String header) {
    Object value = amqpMessage.getMessageProperties().getHeader(header);
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text) {
      try {
        return Long.parseLong(text.trim());
      } catch (NumberFormatException e) {
        logger.debug("Ignoring unparseable {} header: {}", header, text);
      }
    }
    return null;
  }

  private static String requireString(Map<String, Object> message, String field) {
    Object value = message.get(field);
    if (value == null) {
      throw new IllegalArgumentException("missing field: " + field);
    }
    return value.toString();
  }

  private static int requireInt(Map<String, Object> message, String field) {
    Object value = message.get(field);
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text) {
      return Integer.parseInt(text);
    }
    throw new IllegalArgumentException("missing or non-numeric field: " + field);
  }
}
