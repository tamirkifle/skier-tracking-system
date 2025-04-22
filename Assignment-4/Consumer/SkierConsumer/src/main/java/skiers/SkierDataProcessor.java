package skiers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import skiers.model.LiftRide;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class SkierDataProcessor {
  private static final Logger logger = LoggerFactory.getLogger(SkierDataProcessor.class);

  private final BlockingQueue<WriteTask> writeQueue = new LinkedBlockingQueue<>(Constants.MAX_QUEUE_SIZE);
  private final AmazonDynamoDB amazonDynamoDB;
  private volatile boolean running = true;

  @Autowired
  public SkierDataProcessor(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
    startConsumerThread();
  }

  @Async
  public void addSkierEvent(String skierId, LiftRide event, String resortId, String seasonId, String dayId) {
    try {
      writeQueue.put(new WriteTask(skierId, event, resortId, seasonId, dayId));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted while queueing write task", e);
    }
  }

  private void startConsumerThread() {
    Thread consumerThread = new Thread(() -> {
      DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
      Table liftRidesTable = dynamoDB.getTable("LiftRides");
      while (running || !writeQueue.isEmpty()) {
        try {
          WriteTask task = writeQueue.take();
          // Write to LiftRides table
          String compositeKey = task.resortId + "#" + task.seasonId + "#" + task.dayId + "#" + task.event.getTime();
          // GSI key
          String gsiKey = task.resortId + "#" + task.seasonId + "#" + task.dayId;
          String vertical = String.valueOf(task.event.getLiftID() * 10);

          Item liftRideItem = new Item()
              .withPrimaryKey("skierID", task.skierId)
              .withString("resortID#seasonID#dayID#timestamp", compositeKey)
              .withString("dayID", task.dayId)
              .withString("liftID", String.valueOf(task.event.getLiftID()))
              .withString("resortID", task.resortId)
              .withString("seasonID", task.seasonId)
              .withString("timestamp", String.valueOf(task.event.getTime()))
              .withString("vertical", vertical)
              // add GSI to query by resortID seasonID dayID
              .withString("resortSeasonDay", gsiKey);

          liftRidesTable.putItem(liftRideItem);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Consumer thread interrupted", e);
        } catch (Exception e) {
          logger.error("Error processing write task", e);
        }
      }
    });
    consumerThread.setDaemon(true);
    consumerThread.setName("DynamoDB-Write-Consumer");
    consumerThread.start();
  }

  public void shutdown() {
    running = false;
  }

  private static class WriteTask {
    final String skierId;
    final LiftRide event;
    final String resortId;
    final String seasonId;
    final String dayId;

    WriteTask(String skierId, LiftRide event, String resortId, String seasonId, String dayId) {
      this.skierId = skierId;
      this.event = event;
      this.resortId = resortId;
      this.seasonId = seasonId;
      this.dayId = dayId;
    }
  }
}