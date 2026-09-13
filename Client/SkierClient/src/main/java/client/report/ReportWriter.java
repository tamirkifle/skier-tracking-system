package client.report;

import client.scenario.Scenario;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a run into artifacts a reader can evaluate without re-running it: a markdown report, an SVG
 * of the percentile distribution, and HdrHistogram {@code .hgrm} files of the raw distribution.
 */
public final class ReportWriter {

  private static final Logger log = LoggerFactory.getLogger(ReportWriter.class);

  private static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneOffset.UTC);

  private final Path outputDir;

  public ReportWriter(Path outputDir) {
    this.outputDir = outputDir;
  }

  public Path write(Scenario scenario, String baseUrl, List<PhaseResult> results)
      throws IOException {
    Files.createDirectories(outputDir);

    for (PhaseResult result : results) {
      writeHistogram(result);
    }
    Path svg = outputDir.resolve("latency-distribution.svg");
    Files.writeString(svg, LatencyChart.render(results), StandardCharsets.UTF_8);

    Path markdown = outputDir.resolve("report.md");
    Files.writeString(markdown, renderMarkdown(scenario, baseUrl, results), StandardCharsets.UTF_8);

    log.info("Report written to {}", markdown.toAbsolutePath());
    return markdown;
  }

  private void writeHistogram(PhaseResult result) throws IOException {
    Path path = outputDir.resolve("latency-" + slug(result.name()) + ".hgrm");
    // Buffered rather than streamed, so a rendering failure leaves no partial .hgrm behind.
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192);
    try (PrintStream printer = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
      // Values are recorded in microseconds; a scale of 1000 renders the file in milliseconds.
      result.latency().outputPercentileDistribution(printer, 1000.0);
    }
    Files.write(path, buffer.toByteArray());
  }

  private String renderMarkdown(Scenario scenario, String baseUrl, List<PhaseResult> results) {
    StringBuilder out = new StringBuilder(8192);

    out.append("# Benchmark: ").append(scenario.getName()).append("\n\n");
    if (!scenario.getDescription().isBlank()) {
      out.append(scenario.getDescription().trim()).append("\n\n");
    }

    out.append("## Run\n\n")
        .append("| | |\n|---|---|\n")
        .append(row("Started", TIMESTAMP.format(Instant.now())))
        .append(row("Target", "`" + baseUrl + "`"))
        .append(row("Scenario", scenario.getName()))
        .append(row("Skier population", format(scenario.getWorkload().getSkiers())))
        .append(
            row(
                "Resort / season / day",
                scenario.getWorkload().getResortId()
                    + " / "
                    + scenario.getWorkload().getSeasonId()
                    + " / "
                    + scenario.getWorkload().getDayId()))
        .append(row("Max connections", format(scenario.getHttp().getMaxConnections())))
        .append(row("Retry budget", scenario.getHttp().getMaxRetries() + " per request"))
        .append(
            row(
                "JVM",
                System.getProperty("java.version")
                    + " ("
                    + System.getProperty("java.vm.name")
                    + ")"))
        .append(row("Cores", String.valueOf(Runtime.getRuntime().availableProcessors())))
        .append('\n');

    out.append("## Results\n\n")
        .append(
            "| Phase | Mode | Issued | OK | Failed | Wall (s) | Throughput (req/s) "
                + "| p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |\n")
        .append("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n");

    for (PhaseResult result : results) {
      out.append("| ")
          .append(
              result.warmup() ? "_" + result.name() + "_ (warmup)" : "**" + result.name() + "**")
          .append(" | ")
          .append(result.mode())
          .append(" | ")
          .append(format(result.requestsIssued()))
          .append(" | ")
          .append(format(result.successes()))
          .append(" | ")
          .append(format(result.failures()))
          .append(" | ")
          .append(decimal(result.wallSeconds()))
          .append(" | ")
          .append(decimal(result.throughput()))
          .append(" | ")
          .append(millis(result.p50Micros()))
          .append(" | ")
          .append(millis(result.p95Micros()))
          .append(" | ")
          .append(millis(result.p99Micros()))
          .append(" | ")
          .append(millis(result.p999Micros()))
          .append(" | ")
          .append(millis(result.latency().getMaxValue()))
          .append(" |\n");
    }
    out.append("\nWarmup phases are excluded from every aggregate below.\n\n");

    out.append("![Latency distribution](latency-distribution.svg)\n\n");

    out.append("## Little's Law cross-check\n\n")
        .append(
            "`L = λW`, so an independently sampled mean concurrency divided by the mean latency "
                + "should reproduce the measured throughput. This is a **consistency check on the "
                + "measurement**, not a second throughput result, both figures come from the same run, "
                + "so agreement means the instrumentation is coherent and divergence means something was "
                + "sampled badly.\n\n")
        .append(
            "| Phase | Mean concurrency (sampled) | Mean latency (ms) | Measured (req/s) "
                + "| Little's Law (req/s) | Divergence |\n")
        .append("|---|--:|--:|--:|--:|--:|\n");

    for (PhaseResult result : results) {
      if (result.warmup()) {
        continue;
      }
      out.append("| ")
          .append(result.name())
          .append(" | ")
          .append(decimal(result.meanConcurrency()))
          .append(" | ")
          .append(decimal(result.latency().getMean() / 1000d))
          .append(" | ")
          .append(decimal(result.throughput()))
          .append(" | ")
          .append(decimal(result.littlesLawThroughput()))
          .append(" | ")
          .append(percent(result.littlesLawDivergence()))
          .append(" |\n");
    }
    out.append('\n');

    boolean anyOpenLoop = results.stream().anyMatch(r -> r.targetRatePerSecond() > 0);
    if (anyOpenLoop) {
      out.append("## Arrival-rate attainment\n\n")
          .append(
              "An open-loop phase is only valid if the generator kept its schedule. Both columns "
                  + "are measured over the *dispatch* window, from the phase's start to its last "
                  + "dispatch, excluding the tail drain that follows it, because dividing by a "
                  + "window that includes the drain reports a shortfall belonging to the drain.\n\n")
          .append(
              "Read the last two columns first. A late dispatch is one request the schedule asked "
                  + "for and the client could not send on time, which is the direct measurement; "
                  + "attainment is a ratio of rates and anything that lengthens the window moves "
                  + "it. Latencies are charged from each request's due time either way, so a late "
                  + "dispatch is included in the latency rather than hidden, but the offered load "
                  + "was less than the label claims.\n\n")
          .append(
              "| Phase | Target (req/s) | Dispatched (req/s) | Attainment | Late dispatches |"
                  + " Worst lateness |\n|---|--:|--:|--:|--:|--:|\n");
      for (PhaseResult result : results) {
        if (result.targetRatePerSecond() <= 0) {
          continue;
        }
        out.append("| ")
            .append(result.name())
            .append(" | ")
            .append(format(result.targetRatePerSecond()))
            .append(" | ")
            .append(decimal(result.requestsIssued() / Math.max(0.001, result.dispatchSeconds())))
            .append(" | ")
            .append(percent(result.rateAttainment()))
            .append(" | ")
            .append(format(result.lateDispatches()))
            .append(" (")
            .append(percent(result.lateDispatchRate()))
            .append(") | ")
            .append(format(result.maxLatenessMillis()))
            .append(" ms |\n");
      }
      out.append('\n');
    }

    out.append("## Response codes\n\n| Phase | Code | Count | Meaning |\n|---|--:|--:|---|\n");
    for (PhaseResult result : results) {
      for (Map.Entry<Integer, Long> entry : result.statusCounts().entrySet()) {
        out.append("| ")
            .append(result.name())
            .append(" | ")
            .append(entry.getKey())
            .append(" | ")
            .append(format(entry.getValue()))
            .append(" | ")
            .append(describeStatus(entry.getKey()))
            .append(" |\n");
      }
    }
    out.append("\nRetried attempts: ")
        .append(format(results.stream().mapToLong(PhaseResult::retries).sum()))
        .append(" (attempts, not requests, a retried request is still one request).\n\n");

    out.append("## Percentile detail (measured phases)\n\n");
    Histogram combined = combine(results);
    out.append("| Percentile | Latency (ms) |\n|---|--:|\n");
    for (double percentile : new double[] {50, 75, 90, 95, 99, 99.9, 99.99, 100}) {
      out.append("| p")
          .append(trimPercentile(percentile))
          .append(" | ")
          .append(millis(combined.getValueAtPercentile(percentile)))
          .append(" |\n");
    }

    out.append(
            "\n## How to reproduce\n\n```bash\nmake up\njava -jar Client/SkierClient/target/skier-client.jar run \\\n")
        .append("  --scenario benchmarks/scenarios/")
        .append(slug(scenario.getName()))
        .append(".yaml \\\n")
        .append("  --base-url ")
        .append(baseUrl)
        .append(" \\\n  --out benchmarks/out\n```\n");

    out.append("\n## Caveats\n\n")
        .append("- Latency is measured client-side and includes network time to the target.\n")
        .append(
            "- A `201` means the broker accepted the event, not that it is queryable. "
                + "End-to-end pipeline lag is a separate measurement.\n")
        .append(
            "- Open-loop phases use uniform, not Poisson, arrivals; real traffic is burstier, so "
                + "queueing here is understated.\n")
        .append(
            "- Retries are included in a request's measured latency, which is what a client "
                + "experiences, but it means a run with many retries has a heavier tail than the server's "
                + "own per-request timing would show.\n");

    return out.toString();
  }

  private static Histogram combine(List<PhaseResult> results) {
    Histogram combined = new Histogram(3_600_000_000L, 3);
    results.stream()
        .filter(result -> !result.warmup())
        .forEach(result -> combined.add(result.latency()));
    return combined;
  }

  private static String describeStatus(int status) {
    return switch (status) {
      case 200 -> "OK";
      case 201 -> "Created, broker accepted the event";
      case 400 -> "Rejected by validation (not retried)";
      case 429 -> "Shed by admission control";
      case 503 -> "Broker unavailable";
      case 597 -> "Cancelled locally";
      case 598 -> "Socket timeout, no response";
      case 599 -> "Transport failure";
      default -> status >= 500 ? "Server error" : "-";
    };
  }

  private static String row(String key, String value) {
    return "| " + key + " | " + value + " |\n";
  }

  private static String format(long value) {
    return String.format(Locale.ROOT, "%,d", value);
  }

  private static String decimal(double value) {
    return String.format(Locale.ROOT, "%,.1f", value);
  }

  private static String millis(long micros) {
    return String.format(Locale.ROOT, "%,.2f", micros / 1000d);
  }

  private static String percent(double fraction) {
    return String.format(Locale.ROOT, "%.1f%%", fraction * 100);
  }

  private static String trimPercentile(double percentile) {
    return percentile == Math.floor(percentile)
        ? String.valueOf((long) percentile)
        : String.valueOf(percentile);
  }

  static String slug(String value) {
    return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "");
  }
}
