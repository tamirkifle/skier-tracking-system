package client.scenario;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Reads and validates a scenario file. */
public final class ScenarioLoader {

  private static final ObjectMapper MAPPER =
      new ObjectMapper(new YAMLFactory())
          // kebab-case in the YAML, camelCase in the Java class.
          .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private ScenarioLoader() {}

  public static Scenario load(Path path) throws IOException {
    if (!Files.isRegularFile(path)) {
      throw new IOException("Scenario file not found: " + path.toAbsolutePath());
    }
    Scenario scenario = MAPPER.readValue(path.toFile(), Scenario.class);
    validate(scenario, path);
    return scenario;
  }

  public static Scenario parse(String yaml) throws IOException {
    Scenario scenario = MAPPER.readValue(yaml, Scenario.class);
    validate(scenario, Path.of("<inline>"));
    return scenario;
  }

  private static void validate(Scenario scenario, Path source) {
    if (scenario.getPhases().isEmpty()) {
      throw new IllegalArgumentException(source + ": scenario declares no phases");
    }

    for (Scenario.Phase phase : scenario.getPhases()) {
      switch (phase.getMode()) {
        case OPEN_LOOP -> {
          if (phase.getTargetRatePerSecond() <= 0) {
            throw new IllegalArgumentException(
                source
                    + ": open-loop phase '"
                    + phase.getName()
                    + "' needs target-rate-per-second");
          }
          if (phase.getDurationSeconds() <= 0) {
            throw new IllegalArgumentException(
                source + ": open-loop phase '" + phase.getName() + "' needs duration-seconds");
          }
        }
        case CLOSED_LOOP -> {
          if (phase.getThreads() <= 0) {
            throw new IllegalArgumentException(
                source + ": closed-loop phase '" + phase.getName() + "' needs threads > 0");
          }
          if (phase.getRequestsPerThread() <= 0) {
            throw new IllegalArgumentException(
                source
                    + ": closed-loop phase '"
                    + phase.getName()
                    + "' needs requests-per-thread > 0");
          }
        }
      }
    }

    if (scenario.measuredRequests() == 0) {
      throw new IllegalArgumentException(
          source + ": every phase is a warmup, so nothing would be measured");
    }

    Scenario.Workload workload = scenario.getWorkload();
    if (workload.getSkiers() <= 0
        || workload.getLifts() <= 0
        || workload.getMinutesInSkiDay() <= 0) {
      throw new IllegalArgumentException(source + ": workload dimensions must all be positive");
    }
  }
}
