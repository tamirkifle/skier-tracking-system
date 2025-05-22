package skiers;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import skiers.model.LiftRide;

import java.util.Map;

@Service
public class SkierConsumer {
  private static final Logger logger = LoggerFactory.getLogger(SkierConsumer.class);

  private final SkierDataProcessor dataProcessor;
  private final RabbitTemplate rabbitTemplate;

  @Autowired
  public SkierConsumer(SkierDataProcessor dataProcessor, RabbitTemplate rabbitTemplate) {
    this.dataProcessor = dataProcessor;
    this.rabbitTemplate = rabbitTemplate;
  }

  @RabbitListener(queues = Constants.MAIN_QUEUE, concurrency = "" + Constants.CONCURRENT_CONSUMERS, ackMode = "MANUAL")
  public void processMessage(Map<String, Object> message, Message amqpMessage, Channel channel) {
    long deliveryTag = amqpMessage.getMessageProperties().getDeliveryTag();
    try {
      String skierId = (String) message.get("skierID");
      String resortId = (String) message.get("resortID");
      String seasonId = (String) message.get("seasonID");
      String dayId = (String) message.get("dayID");

      LiftRide ride = new LiftRide();
      ride.setLiftID((Integer) message.get("liftID"));
      ride.setTime((Integer) message.get("time"));

      dataProcessor.addSkierEvent(skierId, ride, resortId, seasonId, dayId);

      // Manually acknowledge the message
      channel.basicAck(deliveryTag, false);
    } catch (Exception e) {
      logger.error("Error processing message", e);
      try {
        // Reject the message and don't requeue it (send to DLQ)
        channel.basicReject(deliveryTag, false);
      } catch (Exception ex) {
        logger.error("Error rejecting message", ex);
      }
    }
  }
}