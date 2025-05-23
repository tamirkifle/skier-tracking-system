package client.clientpart1;

import client.common.model.LiftRideEvent;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGenerator implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);
  private final BlockingQueue<LiftRideEvent> queue;
  private final int totalEvents;
  private final Random random = new Random();
  private final String baseUrl;
  private static final int QUEUE_CAPACITY = 1000;

  // Track generation status
  private volatile int eventsGenerated = 0;
  private final AtomicBoolean generationCompleted = new AtomicBoolean(false);

  public EventGenerator(int totalEvents, String baseUrl) {
    this.queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    this.totalEvents = totalEvents;
    this.baseUrl = baseUrl;
  }

  public BlockingQueue<LiftRideEvent> getQueue() {
    return queue;
  }

  public boolean isGenerationComplete() {
    return generationCompleted.get();
  }

  @Override
  public void run() {
    try {
      for (eventsGenerated = 0; eventsGenerated < totalEvents; eventsGenerated++) {
        LiftRideEvent event = generateEvent();
        queue.put(event);
      }
      generationCompleted.set(true);
      log.info("Event generation completed. Total events generated: {}", eventsGenerated);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Event generation interrupted: {}", e.getMessage());
    }
  }


/*  Changes dor Assignment 4
  1. keep resort and season constant in all 3 runs
  resortId = 5000, SeasonId = 2025
  2. vary the day value for each run, namely {1, 2, 3}
  change to 1,2,3 for each run
  3. keep the number of skiers, lift rides as default values for first 3 assignments
  4. Thread configuration of your choice*/
  private LiftRideEvent generateEvent() {
    return new LiftRideEvent(
        random.nextInt(100000) + 1,
        5000,
        random.nextInt(40) + 1,
        2025,
        1,
        random.nextInt(360) + 1,
        baseUrl
    );
  }
}