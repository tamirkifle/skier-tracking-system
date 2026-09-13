package client.generator;

import client.common.model.LiftRideEvent;
import client.scenario.Scenario;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class EventFactory {

  private final Scenario.Workload workload;

  private final String[] ingestUrls;

  private final AtomicLong cursor = new AtomicLong();

  private final Random seeded;

  public EventFactory(Scenario.Workload workload, String baseUrls) {
    this(workload, Arrays.asList(baseUrls.split(",")));
  }

  public EventFactory(Scenario.Workload workload, List<String> baseUrls) {
    this.workload = workload;
    this.ingestUrls =
        baseUrls.stream()
            .map(String::trim)
            .filter(url -> !url.isEmpty())
            .map(url -> url.endsWith("/") ? url.substring(0, url.length() - 1) : url)
            .map(url -> url + "/skiers")
            .toArray(String[]::new);
    if (this.ingestUrls.length == 0) {
      throw new IllegalArgumentException("No target URL in: " + baseUrls);
    }
    this.seeded = new Random(workload.getSeed());
  }

  public LiftRideEvent next() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    return build(
        random.nextInt(workload.getSkiers()) + 1,
        random.nextInt(workload.getLifts()) + 1,
        random.nextInt(workload.getMinutesInSkiDay()) + 1);
  }

  public LiftRideEvent nextDeterministic() {
    return build(
        seeded.nextInt(workload.getSkiers()) + 1,
        seeded.nextInt(workload.getLifts()) + 1,
        seeded.nextInt(workload.getMinutesInSkiDay()) + 1);
  }

  private LiftRideEvent build(int skierId, int liftId, int minute) {
    return new LiftRideEvent(
        skierId,
        workload.getResortId(),
        liftId,
        workload.getSeasonId(),
        workload.getDayId(),
        minute,
        nextIngestUrl());
  }

  private String nextIngestUrl() {
    return ingestUrls.length == 1
        ? ingestUrls[0]
        : ingestUrls[Math.floorMod(cursor.getAndIncrement(), ingestUrls.length)];
  }
}
