package skiers.ingest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import skiers.Constants;
import skiers.config.ConsumerProperties;

/**
 * {@link RetryRouter} over the broker's own delay-and-dead-letter path. The copy goes to {@link
 * Constants#RETRY_QUEUE}, which holds it for its {@code x-message-ttl} and dead-letters it back to
 * the main routing key, so no writer thread sleeps out the delay. It returns only once the broker
 * has confirmed the copy, because the caller acks the original on the strength of that answer.
 */
@Component
public class AmqpRetryRouter implements RetryRouter {

  private static final Logger logger = LoggerFactory.getLogger(AmqpRetryRouter.class);

  private final RabbitTemplate rabbitTemplate;
  private final long confirmTimeoutMs;

  public AmqpRetryRouter(RabbitTemplate rabbitTemplate, ConsumerProperties properties) {
    this.rabbitTemplate = rabbitTemplate;
    this.confirmTimeoutMs = properties.getWriter().getRetryConfirmTimeoutMs();
  }

  @Override
  public boolean republish(Message original, int attempt) {
    // Copied rather than re-encoded, so the retry is byte-identical apart from the attempt count.
    MessageProperties properties = new MessageProperties();
    original.getMessageProperties().getHeaders().forEach(properties::setHeader);
    properties.setHeader(Constants.HEADER_ATTEMPTS, attempt);
    properties.setContentType(original.getMessageProperties().getContentType());
    properties.setContentEncoding(original.getMessageProperties().getContentEncoding());
    properties.setMessageId(original.getMessageProperties().getMessageId());
    // The main queue's messages are persistent by the server; the copy has to say so itself.
    properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);

    Message copy = new Message(original.getBody(), properties);
    String correlationId =
        properties.getMessageId() == null ? "retry-" + attempt : properties.getMessageId();
    CorrelationData correlation = new CorrelationData(correlationId);

    try {
      rabbitTemplate.send(
          Constants.LIFT_RIDE_EXCHANGE, Constants.RETRY_ROUTING_KEY, copy, correlation);
    } catch (AmqpException e) {
      logger.warn("Retry publish for {} failed: {}", correlationId, e.getMessage());
      return false;
    }

    try {
      CorrelationData.Confirm confirm =
          correlation.getFuture().get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
      if (correlation.getReturned() != null) {
        logger.error(
            "Retry route is unroutable ({}): the retry queue's binding is missing",
            correlation.getReturned().getReplyText());
        return false;
      }
      return confirm != null && confirm.isAck();
    } catch (TimeoutException | ExecutionException e) {
      logger.warn("No confirm for retry of {} within {}ms", correlationId, confirmTimeoutMs);
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
