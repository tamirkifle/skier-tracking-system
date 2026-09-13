package client.generator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import client.common.model.LiftRideEvent;
import client.scenario.Scenario;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EventFactoryTest {

  private static Scenario.Workload workload() {
    Scenario.Workload workload = new Scenario.Workload();
    workload.setSkiers(1000);
    workload.setResortId(5);
    workload.setSeasonId(2025);
    workload.setDayId(1);
    workload.setLifts(40);
    workload.setMinutesInSkiDay(360);
    workload.setSeed(42L);
    return workload;
  }

  @Test
  @DisplayName("generated events stay inside the API's documented domain")
  void respectsWorkloadBounds() {
    EventFactory factory = new EventFactory(workload(), "http://localhost:8080");

    for (int i = 0; i < 20_000; i++) {
      LiftRideEvent event = factory.next();
      assertThat(event.getSkierID()).isBetween(1, 1000);
      assertThat(event.getLiftID()).isBetween(1, 40);
      assertThat(event.getTime()).isBetween(1, 360);
      assertThat(event.getResortID()).isEqualTo(5);
      assertThat(event.getSeasonID()).isEqualTo(2025);
      assertThat(event.getDayID()).isEqualTo(1);
    }
  }

  @Test
  @DisplayName("the URL matches the ingest route exactly")
  void buildsTheIngestUrl() {
    EventFactory factory = new EventFactory(workload(), "http://localhost:8080");
    LiftRideEvent event = factory.next();

    assertThat(event.getFormattedUrl())
        .isEqualTo(
            "http://localhost:8080/skiers/5/seasons/2025/days/1/skier/" + event.getSkierID());
  }

  @Test
  @DisplayName("a trailing slash on the base URL does not produce a double slash")
  void normalisesTrailingSlash() {
    EventFactory factory = new EventFactory(workload(), "http://localhost:8080/");
    assertThat(factory.next().getFormattedUrl()).doesNotContain("//skiers").contains("/skiers/5/");
  }

  @Test
  @DisplayName("the body is valid JSON with exactly the two fields the API requires")
  void buildsTheRequestBody() {
    EventFactory factory = new EventFactory(workload(), "http://localhost:8080");
    LiftRideEvent event = factory.next();

    assertThat(event.jsonBody())
        .isEqualTo("{\"time\":" + event.getTime() + ",\"liftID\":" + event.getLiftID() + "}");
  }

  @Test
  @DisplayName("the seeded path is reproducible so two runs compare the same workload")
  void seededPathIsDeterministic() {
    EventFactory first = new EventFactory(workload(), "http://localhost:8080");
    EventFactory second = new EventFactory(workload(), "http://localhost:8080");

    for (int i = 0; i < 100; i++) {
      LiftRideEvent a = first.nextDeterministic();
      LiftRideEvent b = second.nextDeterministic();
      assertThat(a.getSkierID()).isEqualTo(b.getSkierID());
      assertThat(a.getLiftID()).isEqualTo(b.getLiftID());
      assertThat(a.getTime()).isEqualTo(b.getTime());
    }
  }

  @Test
  @DisplayName("several base URLs are round-robined, so a fleet gets an even share each")
  void spreadsAcrossReplicas() {
    EventFactory factory =
        new EventFactory(workload(), "http://localhost:8080, http://localhost:8081/");

    Map<String, Integer> perTarget = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      String url = factory.next().getFormattedUrl();
      perTarget.merge(url.substring(0, url.indexOf("/skiers")), 1, Integer::sum);
    }

    // Exact, not approximate: an uneven split would measure the generator, not the controllers.
    assertThat(perTarget)
        .containsOnly(entry("http://localhost:8080", 500), entry("http://localhost:8081", 500));
  }

  @Test
  @DisplayName("the skier population is spread across, not clustered")
  void spreadsAcrossThePopulation() {
    EventFactory factory = new EventFactory(workload(), "http://localhost:8080");
    Set<Integer> skiers = new HashSet<>();
    for (int i = 0; i < 10_000; i++) {
      skiers.add(factory.next().getSkierID());
    }
    assertThat(skiers).hasSizeGreaterThan(900);
  }
}
