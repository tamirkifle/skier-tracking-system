package skiers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SkierDataProcessor {
  private static final Logger logger = LoggerFactory.getLogger(SkierDataProcessor.class);

  // Number of concurrent writer threads
  private static final int NUM_WRITER_THREADS = 100;

  // Maximum number of retry attempts for failed writes
  private static final int MAX_RETRY_ATTEMPTS = 3;

  // Base delay for exponential backoff (in milliseconds)
  private static final long BASE_RETRY_DELAY_MS = 100;

  private final AtomicInteger uniqueSkierTrackingCount = new AtomicInteger(0);

  private final BlockingQueue<WriteTask> writeQueue = new LinkedBlockingQueue<>(Constants.MAX_QUEUE_SIZE);
  private final AmazonDynamoDB amazonDynamoDB;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Thread> consumerThreads = new ArrayList<>();
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger errorCount = new AtomicInteger(0);
  private final AtomicInteger retryCount = new AtomicInteger(0);
  private final AtomicInteger permanentFailureCount = new AtomicInteger(0);

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
          WriteTask task = null;
          try {
            task = writeQueue.poll(100, TimeUnit.MILLISECONDS);
            if (task == null) {
              // If no tasks, continue and check running status again
              continue;
            }

            processWriteTask(liftRidesTable, task, threadName);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Consumer thread interrupted", e);
            break;
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

  private void processWriteTask(Table liftRidesTable, WriteTask task, String threadName) {
    try {
      // Create DynamoDB item
      String compositeKey = task.resortId + "#" + task.seasonId + "#" + task.dayId + "#" + task.event.getTime();
      String vertical = String.valueOf(task.event.getLiftID() * 10);

      // Additional composite keys for GSIs
      String skierSeasonKey = task.skierId + "#" + task.seasonId;
      String resortDayKey = task.resortId + "#" + task.dayId;
      String resortSkierKey = task.resortId + "#" + task.skierId;
      String seasonDayKey = task.seasonId + "#" + task.dayId;
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
          .withString("resortID#dayID", resortDayKey)
          .withString("resortID#skierID", resortSkierKey)
          .withString("seasonID#dayID", seasonDayKey)
          .withString("resortSeasonDay", resortSeasonDayKey);

      liftRidesTable.putItem(liftRideItem);

      liftRidesTable.putItem(liftRideItem);

      // NEW CODE: Update the skier count table
      updateSkierCountForResortSeasonDay(task.skierId, task.resortId, task.seasonId, task.dayId);

      int success = successCount.incrementAndGet();
      if (success % 1000 == 0) {
        logger.info("Successfully processed {} write tasks (retries: {}, permanent failures: {})",
            success, retryCount.get(), permanentFailureCount.get());
      }

    } catch (AmazonServiceException e) {
      // These are retryable exceptions
      handleRetryableException(task, e, threadName);
    } catch (Exception e) {
      // Non-retryable exceptions result in permanent failure
      permanentFailureCount.incrementAndGet();
      errorCount.incrementAndGet();
      logger.error("Permanent error processing write task in thread {}: {}", threadName, e.getMessage());
    }
  }

  // Track unique skiers seen for each resort/season/day
  private final Map<String, Set<String>> uniqueSkiersByResortSeasonDay = new ConcurrentHashMap<>();

  // Track unique skiers for a resort/season/day with distributed idempotency
  private void updateSkierCountForResortSeasonDay(String skierId, String resortId, String seasonId, String dayId) {
    String resortSeasonDayKey = String.format("%s#%s#%s", resortId, seasonId, dayId);
    String skierKey = String.format("%s#%s#%s#%s", resortId, seasonId, dayId, skierId);

    try {
      DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

      // First, check if this skier has already been counted for this resort/season/day
      Table skierTrackingTable = dynamoDB.getTable("SkierTracking");

      try {
        // Try to insert a record that indicates this skier has been counted
        Item skierTracker = new Item()
            .withPrimaryKey("skierKey", skierKey)
            .withString("resortSeasonDay", resortSeasonDayKey)
            .withBoolean("counted", true);

        // Only insert if the item doesn't already exist - this ensures idempotency
        PutItemSpec putSpec = new PutItemSpec()
            .withItem(skierTracker)
            .withConditionExpression("attribute_not_exists(skierKey)");

        skierTrackingTable.putItem(putSpec);

        // If we get here without an exception, this is the first time we've seen this skier
        // Now increment the counter in the main table
        Table skierCountsTable = dynamoDB.getTable("SkierCounts");

        try {
          // Try to update existing count
          UpdateItemSpec updateSpec = new UpdateItemSpec()
              .withPrimaryKey("resortSeasonDay", resortSeasonDayKey)
              .withUpdateExpression("ADD uniqueSkierCount :val")
              .withValueMap(new ValueMap().withNumber(":val", 1));

          skierCountsTable.updateItem(updateSpec);
        } catch (ResourceNotFoundException e) {
          // Count record doesn't exist yet, create it
          Item newCount = new Item()
              .withPrimaryKey("resortSeasonDay", resortSeasonDayKey)
              .withNumber("uniqueSkierCount", 1);

          skierCountsTable.putItem(newCount);
        }

        // Log every 1000th new unique skier
        int trackingCount = uniqueSkierTrackingCount.incrementAndGet();
        if (trackingCount % 1000 == 0) {
          logger.info("Tracked {} unique skiers across all resorts/seasons/days", trackingCount);
        }

      } catch (ConditionalCheckFailedException e) {
        // This skier has already been counted, do nothing
        // This is the expected path for skiers we've already seen
      }
    } catch (Exception e) {
      logger.error("Error updating skier count for {}#{}: {}", resortSeasonDayKey, skierId, e.getMessage());
      // Don't rethrow - we don't want to fail the main operation
    }
  }

  private void handleRetryableException(WriteTask task, Exception e, String threadName) {
    // Increment the retry attempt count
    task.retryCount++;

    if (task.retryCount <= MAX_RETRY_ATTEMPTS) {
      retryCount.incrementAndGet();

      // Calculate exponential backoff delay
      long delayMs = BASE_RETRY_DELAY_MS * (long)Math.pow(2, task.retryCount - 1);

      // Log retry attempt
      logger.warn("Retryable error in thread {}. Retry attempt {}/{} after {}ms delay. Error: {}",
          threadName, task.retryCount, MAX_RETRY_ATTEMPTS, delayMs, e.getMessage());

      // Put the task back in the queue after a delay
      try {
        // Simple way to implement delay - in production you might want a more sophisticated approach
        Thread.sleep(delayMs);
        writeQueue.put(task);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        logger.error("Interrupted while requeueing write task", ie);
      }
    } else {
      // We've reached the maximum number of retry attempts
      permanentFailureCount.incrementAndGet();
      errorCount.incrementAndGet();
      logger.error("Exceeded maximum retry attempts ({}) for task in thread {}: {}",
          MAX_RETRY_ATTEMPTS, threadName, e.getMessage());
    }
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

    logger.info("SkierDataProcessor shutdown complete. Success: {}, Errors: {}, Retries: {}, Permanent Failures: {}, Queue remaining: {}",
        successCount.get(), errorCount.get(), retryCount.get(), permanentFailureCount.get(), writeQueue.size());
  }

  // Get statistics about the processor
  public ProcessorStats getStats() {
    return new ProcessorStats(
        consumerThreads.size(),
        successCount.get(),
        errorCount.get(),
        retryCount.get(),
        permanentFailureCount.get(),
        writeQueue.size()
    );
  }

  // Stats class for monitoring
  public static class ProcessorStats {
    private final int activeThreads;
    private final int successCount;
    private final int errorCount;
    private final int retryCount;
    private final int permanentFailureCount;
    private final int queueSize;

    public ProcessorStats(int activeThreads, int successCount, int errorCount,
        int retryCount, int permanentFailureCount, int queueSize) {
      this.activeThreads = activeThreads;
      this.successCount = successCount;
      this.errorCount = errorCount;
      this.retryCount = retryCount;
      this.permanentFailureCount = permanentFailureCount;
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

    public int getRetryCount() {
      return retryCount;
    }

    public int getPermanentFailureCount() {
      return permanentFailureCount;
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
    int retryCount = 0;

    WriteTask(String skierId, LiftRide event, String resortId, String seasonId, String dayId) {
      this.skierId = skierId;
      this.event = event;
      this.resortId = resortId;
      this.seasonId = seasonId;
      this.dayId = dayId;
    }
  }
}