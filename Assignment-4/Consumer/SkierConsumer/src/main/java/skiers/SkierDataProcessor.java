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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SkierDataProcessor {
  private static final Logger logger = LoggerFactory.getLogger(SkierDataProcessor.class);

  // Number of concurrent writer threads
  private static final int NUM_WRITER_THREADS = 100;

  private final BlockingQueue<WriteTask> writeQueue = new LinkedBlockingQueue<>(Constants.MAX_QUEUE_SIZE);
  private final AmazonDynamoDB amazonDynamoDB;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Thread> consumerThreads = new ArrayList<>();
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger errorCount = new AtomicInteger(0);

  @Autowired
  public SkierDataProcessor(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
    startConsumerThreads();
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

  private void startConsumerThreads() {
    DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
    Table liftRidesTable = dynamoDB.getTable("LiftRides");

    // Start multiple consumer threads
    for (int i = 0; i < NUM_WRITER_THREADS; i++) {
      Thread consumerThread = new Thread(() -> {
        String threadName = Thread.currentThread().getName();
        logger.info("Starting consumer thread: {}", threadName);

        while (running.get() || !writeQueue.isEmpty()) {
          try {
            WriteTask task = writeQueue.poll();
            if (task == null) {
              // If no tasks, wait a bit and check again
              Thread.sleep(50);
              continue;
            }

            // Write to LiftRides table
            String compositeKey = task.resortId + "#" + task.seasonId + "#" + task.dayId + "#" + task.event.getTime();
            String vertical = String.valueOf(task.event.getLiftID() * 10);

            // Additional composite keys for GSIs
            String skierSeasonKey = task.skierId + "#" + task.seasonId;
            String daySkierKey = task.dayId + "#" + task.skierId;
            String resortDayKey = task.resortId + "#" + task.dayId;
            String resortSeasonKey = task.resortId + "#" + task.seasonId;
            String resortSeasonDayKey = task.resortId + "#" + task.seasonId + "#" + task.dayId;

            Item liftRideItem = new Item()
                .withPrimaryKey("skierID", task.skierId)
                .withString("resortID#seasonID#dayID#timestamp", compositeKey)
                .withString("dayID", task.dayId)
                .withString("liftID", String.valueOf(task.event.getLiftID()))
                .withString("resortID", task.resortId)
                .withString("seasonID", task.seasonId)
                .withString("timestamp", String.valueOf(task.event.getTime()))
                .withString("vertical", vertical)

                // Add the additional composite keys for GSIs
                .withString("skierID#seasonID", skierSeasonKey)
                .withString("dayID#skierID", daySkierKey)
                .withString("resortID#dayID", resortDayKey)
                .withString("resortID#seasonID", resortSeasonKey)
                .withString("resortSeasonDay", resortSeasonDayKey);

            liftRidesTable.putItem(liftRideItem);
            int success = successCount.incrementAndGet();
            if (success % 1000 == 0) {
              logger.info("Successfully processed {} write tasks", success);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Consumer thread interrupted", e);
            break;
          } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("Error processing write task in thread {}: {}", threadName, e.getMessage());
          }
        }
        logger.info("Consumer thread {} shutting down", threadName);
      });

      consumerThread.setDaemon(true);
      consumerThread.setName("DynamoDB-Writer-" + i);
      consumerThread.start();
      consumerThreads.add(consumerThread);
    }

    logger.info("Started {} DynamoDB writer threads", NUM_WRITER_THREADS);
  }

  public void shutdown() {
    logger.info("Shutting down SkierDataProcessor");
    running.set(false);

    // Create a countdown latch to wait for all threads to finish
    CountDownLatch latch = new CountDownLatch(consumerThreads.size());

    // Interrupt all threads to wake them up if they're sleeping
    consumerThreads.forEach(thread -> {
      // Create a separate thread to wait for each consumer thread to finish
      new Thread(() -> {
        try {
          thread.join(5000); // Wait up to 5 seconds for each thread
        } catch (InterruptedException e) {
          logger.warn("Interrupted while waiting for thread {} to finish", thread.getName());
        } finally {
          latch.countDown();
        }
      }).start();
    });

    try {
      // Wait for all threads to finish or timeout
      latch.await();
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for all threads to finish");
    }

    logger.info("SkierDataProcessor shutdown complete. Success: {}, Errors: {}, Queue remaining: {}",
        successCount.get(), errorCount.get(), writeQueue.size());
  }

  // Get statistics about the processor
  public ProcessorStats getStats() {
    return new ProcessorStats(
        consumerThreads.size(),
        successCount.get(),
        errorCount.get(),
        writeQueue.size()
    );
  }

  // Stats class for monitoring
  public static class ProcessorStats {
    private final int activeThreads;
    private final int successCount;
    private final int errorCount;
    private final int queueSize;

    public ProcessorStats(int activeThreads, int successCount, int errorCount, int queueSize) {
      this.activeThreads = activeThreads;
      this.successCount = successCount;
      this.errorCount = errorCount;
      this.queueSize = queueSize;
    }

    public int getActiveThreads() {
      return activeThreads;
    }

    public int getSuccessCount() {
      return successCount;
    }

    public int getErrorCount() {
      return errorCount;
    }

    public int getQueueSize() {
      return queueSize;
    }
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