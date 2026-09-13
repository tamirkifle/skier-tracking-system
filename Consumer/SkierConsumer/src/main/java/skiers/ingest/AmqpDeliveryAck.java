package skiers.ingest;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;

/**
 * {@link DeliveryAck} over a real AMQP channel. {@code Channel} is not thread-safe, so every frame
 * goes out under a lock on it. The {@code settled} guard settles a delivery at most once: an
 * unknown delivery tag is a fatal channel error, not something RabbitMQ ignores.
 */
public final class AmqpDeliveryAck implements DeliveryAck {

  private static final Logger logger = LoggerFactory.getLogger(AmqpDeliveryAck.class);

  private final Channel channel;
  private final long deliveryTag;
  private final Message original;
  private final RetryRouter retryRouter;
  private final AtomicBoolean settled = new AtomicBoolean(false);

  public AmqpDeliveryAck(
      Channel channel, long deliveryTag, Message original, RetryRouter retryRouter) {
    this.channel = channel;
    this.deliveryTag = deliveryTag;
    this.original = original;
    this.retryRouter = retryRouter;
  }

  @Override
  public void ack() {
    if (!settled.compareAndSet(false, true)) {
      return;
    }
    try {
      synchronized (channel) {
        channel.basicAck(deliveryTag, false);
      }
    } catch (IOException | RuntimeException e) {
      // The event is durable; a lost ack costs a redelivery, which the write path absorbs.
      logger.warn("Failed to ack delivery {}: {}", deliveryTag, e.getMessage());
    }
  }

  @Override
  public void reject(boolean requeue) {
    if (!settled.compareAndSet(false, true)) {
      return;
    }
    try {
      synchronized (channel) {
        channel.basicNack(deliveryTag, false, requeue);
      }
    } catch (IOException | RuntimeException e) {
      logger.warn("Failed to nack delivery {}: {}", deliveryTag, e.getMessage());
    }
  }

  /** Settles only once the retry copy is confirmed: reversed, a lost republish drops the event. */
  @Override
  public boolean retryLater(int attempt) {
    if (settled.get()) {
      return false;
    }
    boolean delayed = retryRouter.republish(original, attempt);
    if (delayed) {
      ack();
    } else {
      // Requeued unchanged, so no delay is spent and no attempt is recorded.
      reject(true);
    }
    return delayed;
  }

  long deliveryTag() {
    return deliveryTag;
  }
}
