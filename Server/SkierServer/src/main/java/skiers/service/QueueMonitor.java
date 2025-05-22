package skiers.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import skiers.Constants;

@Service
public class QueueMonitor {
  private static final Logger logger = LoggerFactory.getLogger(QueueMonitor.class);

  @Autowired private RabbitAdmin rabbitAdmin;
  @Autowired private RateLimiter rateLimiter;

  @Scheduled(fixedRate = 50)
  public void adjustRateBasedOnQueueDepth() {
    int currentDepth = getQueueDepth();
    int currentRate = rateLimiter.getCurrentRate();

    if (currentDepth >= Constants.MAX_QUEUE_SIZE) {
      int newRate = Math.max(currentRate / 2, Constants.MIN_RATE);
      rateLimiter.adjustRateDown(newRate);
    } else if (currentDepth > Constants.TARGET_QUEUE_SIZE) {
      int newRate = Math.max(currentRate - 1000, Constants.MIN_RATE);
      rateLimiter.adjustRateDown(newRate);
      } else if (currentDepth < Constants.MIN_QUEUE_SIZE) {
      int newRate = Math.min(currentRate + 10, Constants.MAX_RATE);
      rateLimiter.adjustRateUp(newRate);
       }
  }

  private int getQueueDepth() {
    try {
      var info = rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE); // Use Constants.MAIN_QUEUE
      return (info != null) ? info.getMessageCount() : 0;
    } catch (Exception e) {
      logger.error("Error fetching queue size: {}", e.getMessage());
      return 0;
    }
  }
}