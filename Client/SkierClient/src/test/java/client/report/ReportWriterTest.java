package client.report;

import static org.assertj.core.api.Assertions.assertThat;

import client.scenario.Scenario;
import client.scenario.ScenarioLoader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReportWriterTest {

  private static Histogram histogram(long... micros) {
    Histogram histogram = new Histogram(3_600_000_000L, 3);
    for (long value : micros) {
      histogram.recordValue(value);
    }
    return histogram;
  }

  private static PhaseResult phase(String name, boolean warmup, long... micros) {
    Histogram histogram = histogram(micros);
    return new PhaseResult(
        name,
        warmup,
        "closed loop",
        micros.length,
        micros.length,
        micros.length,
        0,
        0,
        1.0,
        1.0,
        0,
        0,
        histogram,
        List.of(10, 12, 11),
        Map.of(201, (long) micros.length),
        0);
  }

  private static Scenario scenario() throws IOException {
    return ScenarioLoader.parse(
        """
        name: unit-test
        description: a scenario used by ReportWriterTest
        phases:
          - name: steady
            mode: closed-loop
            threads: 2
            requests-per-thread: 5
        """);
  }

  @Test
  @DisplayName("writes a markdown report, an SVG chart and a histogram per phase")
  void writesEveryArtifact(@TempDir Path out) throws Exception {
    List<PhaseResult> results =
        List.of(phase("warmup", true, 1000, 2000), phase("steady", false, 5000, 6000, 90000));

    Path report = new ReportWriter(out).write(scenario(), "http://localhost:8080", results);

    assertThat(report).exists();
    assertThat(out.resolve("latency-distribution.svg")).exists();
    assertThat(out.resolve("latency-warmup.hgrm")).exists();
    assertThat(out.resolve("latency-steady.hgrm")).exists();
  }

  @Test
  @DisplayName("the report records the configuration a number came from")
  void reportIsSelfDescribing() throws Exception {
    Path out = Files.createTempDirectory("report");
    new ReportWriter(out)
        .write(scenario(), "http://example:8080", List.of(phase("steady", false, 5000)));

    String markdown = Files.readString(out.resolve("report.md"));

    assertThat(markdown)
        .contains("unit-test")
        .contains("http://example:8080")
        .contains("How to reproduce")
        .contains("Caveats")
        .contains("Little's Law");
  }

  @Test
  @DisplayName("warmup phases are labelled and excluded from the aggregate percentiles")
  void excludesWarmupFromAggregates() throws Exception {
    Path out = Files.createTempDirectory("report");
    // Warmup is deliberately slow; if it leaked into the aggregate the p100 would be 5,000ms.
    List<PhaseResult> results =
        List.of(phase("warmup", true, 5_000_000), phase("steady", false, 1000, 1000, 1000));

    new ReportWriter(out).write(scenario(), "http://localhost:8080", results);
    String markdown = Files.readString(out.resolve("report.md"));

    assertThat(markdown).contains("(warmup)");
    int detailStart = markdown.indexOf("## Percentile detail");
    assertThat(detailStart).isPositive();
    assertThat(markdown.substring(detailStart)).doesNotContain("5,000.00");
  }

  @Test
  @DisplayName("the SVG is self-contained and renders every measured series")
  void svgIsSelfContained(@TempDir Path out) throws Exception {
    new ReportWriter(out)
        .write(
            scenario(),
            "http://localhost:8080",
            List.of(phase("phase-a", false, 1000, 2000), phase("phase-b", false, 8000, 9000)));

    String svg = Files.readString(out.resolve("latency-distribution.svg"));

    // Must render in a markdown view: no script, no external fetch. The xmlns http URI stays.
    assertThat(svg).startsWith("<svg").endsWith("</svg>\n");
    assertThat(svg)
        .doesNotContain("<script")
        .doesNotContain("<image")
        .doesNotContain("xlink:href")
        .doesNotContain("href=")
        .doesNotContain("@import");
    assertThat(svg).contains("phase-a").contains("phase-b");
    assertThat(svg).doesNotContain("NaN").doesNotContain("Infinity");
  }

  @Test
  @DisplayName("a phase name with awkward characters produces a safe filename")
  void sanitisesFilenames(@TempDir Path out) throws Exception {
    new ReportWriter(out)
        .write(
            scenario(), "http://localhost:8080", List.of(phase("6000 rps / burst!", false, 1000)));

    assertThat(out.resolve("latency-6000-rps-burst.hgrm")).exists();
  }
}
