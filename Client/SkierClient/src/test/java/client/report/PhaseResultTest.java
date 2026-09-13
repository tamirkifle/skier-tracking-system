package client.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PhaseResultTest {

  private static PhaseResult openLoopPhase(
      long issued, double wallSeconds, double dispatchSeconds, long lateDispatches) {
    Histogram histogram = new Histogram(3_600_000_000L, 3);
    histogram.recordValue(1_000);
    return new PhaseResult(
        "sweep",
        false,
        "open loop",
        issued,
        issued,
        issued,
        0,
        0,
        wallSeconds,
        dispatchSeconds,
        lateDispatches,
        lateDispatches == 0 ? 0 : 400,
        histogram,
        List.of(10),
        Map.of(201, issued),
        500);
  }

  @Test
  @DisplayName("a phase that held its schedule reports full attainment despite a long drain")
  void attainmentExcludesTheDrain() {
    // 15,000 at 500/s is 30 s of dispatching plus a 30 s drain; wall time would read 50%.
    PhaseResult result = openLoopPhase(15_000, 60.0, 30.0, 0);

    assertThat(result.rateAttainment()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.001));
    assertThat(result.lateDispatchRate()).isZero();
  }

  @Test
  @DisplayName("a client that fell behind reports it in both the ratio and the late count")
  void attainmentFallsWhenDispatchDoesNotKeepUp() {
    PhaseResult result = openLoopPhase(15_000, 70.0, 40.0, 6_000);

    assertThat(result.rateAttainment()).isCloseTo(0.75, org.assertj.core.data.Offset.offset(0.001));
    assertThat(result.lateDispatchRate())
        .isCloseTo(0.4, org.assertj.core.data.Offset.offset(0.001));
    assertThat(result.maxLatenessMillis()).isEqualTo(400);
  }

  @Test
  @DisplayName("a closed-loop phase has no schedule, so attainment is not a claim about it")
  void closedLoopAttainmentIsOne() {
    Histogram histogram = new Histogram(3_600_000_000L, 3);
    histogram.recordValue(1_000);
    PhaseResult result =
        new PhaseResult(
            "baseline",
            false,
            "closed loop",
            1_000,
            1_000,
            1_000,
            0,
            0,
            10.0,
            10.0,
            0,
            0,
            histogram,
            List.of(10),
            Map.of(201, 1_000L),
            0);

    assertThat(result.rateAttainment()).isEqualTo(1.0);
  }
}
