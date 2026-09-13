package client.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScenarioLoaderTest {

  private static final String VALID =
      """
      name: example
      description: a scenario
      workload:
        skiers: 500
        resort-id: 7
        lifts: 12
        minutes-in-ski-day: 180
      http:
        max-connections: 64
        max-retries: 2
      phases:
        - name: warmup
          mode: closed-loop
          warmup: true
          threads: 2
          requests-per-thread: 5
        - name: steady
          mode: open-loop
          target-rate-per-second: 100
          duration-seconds: 5
      """;

  @Test
  @DisplayName("kebab-case keys bind to camelCase fields")
  void bindsKebabCaseKeys() throws IOException {
    Scenario scenario = ScenarioLoader.parse(VALID);

    assertThat(scenario.getName()).isEqualTo("example");
    assertThat(scenario.getWorkload().getResortId()).isEqualTo(7);
    assertThat(scenario.getWorkload().getMinutesInSkiDay()).isEqualTo(180);
    assertThat(scenario.getHttp().getMaxConnections()).isEqualTo(64);
    assertThat(scenario.getPhases()).hasSize(2);
  }

  @Test
  @DisplayName("kebab-case, snake_case and upper-case mode values all parse")
  void acceptsModeSpellings() {
    // Jackson's naming strategy does not apply to enum constants, so Mode needs its own creator.
    assertThat(Scenario.Mode.fromString("closed-loop")).isEqualTo(Scenario.Mode.CLOSED_LOOP);
    assertThat(Scenario.Mode.fromString("open-loop")).isEqualTo(Scenario.Mode.OPEN_LOOP);
    assertThat(Scenario.Mode.fromString("OPEN_LOOP")).isEqualTo(Scenario.Mode.OPEN_LOOP);
    assertThat(Scenario.Mode.fromString(" Closed-Loop ")).isEqualTo(Scenario.Mode.CLOSED_LOOP);
  }

  @Test
  @DisplayName("warmup phases are excluded from the measured total")
  void excludesWarmupFromMeasuredTotal() throws IOException {
    Scenario scenario = ScenarioLoader.parse(VALID);
    // 2x5 warmup is excluded; 100 req/s for 5s is counted.
    assertThat(scenario.measuredRequests()).isEqualTo(500);
  }

  @Test
  @DisplayName("an unknown key is ignored rather than fatal")
  void toleratesUnknownKeys() throws IOException {
    String yaml =
        """
        name: forward-compatible
        some-future-option: 42
        phases:
          - name: steady
            mode: closed-loop
            threads: 1
            requests-per-thread: 1
        """;
    assertThat(ScenarioLoader.parse(yaml).getName()).isEqualTo("forward-compatible");
  }
}
