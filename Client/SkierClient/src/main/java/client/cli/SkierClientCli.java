package client.cli;

import client.engine.ClosedLoopEngine;
import client.engine.LoadEngine;
import client.engine.OpenLoopEngine;
import client.generator.EventFactory;
import client.metrics.LatencyRecorder;
import client.report.PhaseResult;
import client.report.ReportWriter;
import client.scenario.Scenario;
import client.scenario.ScenarioLoader;
import client.transport.HttpTransport;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "skier-client",
    mixinStandardHelpOptions = true,
    version = "skier-client 2.0.0",
    description = "Load generator and benchmark harness for the skier tracking pipeline.",
    subcommands = {SkierClientCli.RunCommand.class})
public final class SkierClientCli implements Callable<Integer> {

  @Override
  public Integer call() {
    CommandLine.usage(this, System.out);
    return CommandLine.ExitCode.USAGE;
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new SkierClientCli()).execute(args));
  }

  @Command(
      name = "run",
      mixinStandardHelpOptions = true,
      description = "Execute a scenario against a target and write a markdown + SVG report.")
  static final class RunCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(RunCommand.class);

    @Option(
        names = {"-s", "--scenario"},
        required = true,
        description = "Path to a scenario YAML file.")
    Path scenarioPath;

    @Option(
        names = {"-u", "--base-url"},
        defaultValue = "http://localhost:8080",
        description =
            "Target base URL, or several separated by commas to spread the load evenly across a "
                + "replica set (default: ${DEFAULT-VALUE}).")
    String baseUrl;

    @Option(
        names = {"-o", "--out"},
        defaultValue = "benchmarks/out",
        description = "Directory for the report artifacts (default: ${DEFAULT-VALUE}).")
    Path outputDir;

    @Option(
        names = {"--fail-under-success-rate"},
        defaultValue = "0.99",
        description =
            "Exit non-zero if the measured success rate falls below this (default: ${DEFAULT-VALUE}). "
                + "Makes a benchmark usable as a CI gate rather than only as a report.")
    double minSuccessRate;

    @Override
    public Integer call() throws Exception {
      Scenario scenario = ScenarioLoader.load(scenarioPath);

      log.info("Scenario '{}' against {}", scenario.getName(), baseUrl);
      log.info(
          "Measured requests: {}", String.format(Locale.ROOT, "%,d", scenario.measuredRequests()));

      LatencyRecorder recorder = new LatencyRecorder();
      List<PhaseResult> results = new ArrayList<>();

      try (HttpTransport transport = new HttpTransport(scenario.getHttp(), recorder)) {
        EventFactory events = new EventFactory(scenario.getWorkload(), baseUrl);

        for (Scenario.Phase phase : scenario.getPhases()) {
          LoadEngine engine =
              phase.getMode() == Scenario.Mode.OPEN_LOOP
                  ? new OpenLoopEngine()
                  : new ClosedLoopEngine();

          List<Integer> concurrency = new CopyOnWriteArrayList<>();

          long startNanos = System.nanoTime();
          LoadEngine.Dispatch dispatch =
              engine.run(phase, events, transport, recorder, concurrency::add);
          long wallNanos = System.nanoTime() - startNanos;
          long issued = dispatch.issued();

          LatencyRecorder.Interval interval = recorder.snapshot(phase.getName());

          PhaseResult result =
              new PhaseResult(
                  phase.getName(),
                  phase.isWarmup(),
                  phase.getMode() == Scenario.Mode.OPEN_LOOP ? "open loop" : "closed loop",
                  issued,
                  interval.completed(),
                  interval.successes(),
                  interval.failures(),
                  interval.retries(),
                  wallNanos / 1_000_000_000d,
                  dispatch.dispatchSeconds(),
                  dispatch.lateDispatches(),
                  dispatch.maxLatenessMillis(),
                  interval.latency(),
                  List.copyOf(concurrency),
                  interval.statusCounts(),
                  phase.getTargetRatePerSecond());

          results.add(result);
          logPhase(result);

          if (issued != result.requestsCompleted()) {
            log.warn(
                "Phase '{}' issued {} requests but only {} had completed when it closed",
                phase.getName(),
                issued,
                result.requestsCompleted());
          }
        }
      }

      Path report = new ReportWriter(outputDir).write(scenario, baseUrl, results);
      log.info("Report: {}", report.toAbsolutePath());

      double successRate = overallSuccessRate(results);
      log.info("Overall success rate: {}", String.format(Locale.ROOT, "%.4f", successRate));
      if (successRate < minSuccessRate) {
        log.error(
            "Success rate {} is below the required {}",
            String.format(Locale.ROOT, "%.4f", successRate),
            String.format(Locale.ROOT, "%.4f", minSuccessRate));
        return 1;
      }
      return 0;
    }

    private static double overallSuccessRate(List<PhaseResult> results) {
      long completed = 0;
      long successes = 0;
      for (PhaseResult result : results) {
        if (result.warmup()) {
          continue;
        }
        completed += result.requestsCompleted();
        successes += result.successes();
      }
      return completed == 0 ? 0 : (double) successes / completed;
    }

    private static void logPhase(PhaseResult result) {
      log.info(
          "  {}{}: issued={} completed={} wall={}s throughput={} req/s p50={}ms p99={}ms p99.9={}ms",
          result.name(),
          result.warmup() ? " (warmup, excluded)" : "",
          result.requestsIssued(),
          result.requestsCompleted(),
          String.format(Locale.ROOT, "%.1f", result.wallSeconds()),
          String.format(Locale.ROOT, "%.0f", result.throughput()),
          String.format(Locale.ROOT, "%.2f", result.p50Micros() / 1000d),
          String.format(Locale.ROOT, "%.2f", result.p99Micros() / 1000d),
          String.format(Locale.ROOT, "%.2f", result.p999Micros() / 1000d));
    }
  }
}
