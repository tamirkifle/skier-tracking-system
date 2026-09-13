package client.report;

import java.util.List;
import java.util.Locale;

/**
 * Renders latency percentiles as a standalone SVG. It is hand-written because the output is
 * committed and read inside markdown on a code host: one file, no runtime, no CDN, no JavaScript.
 *
 * <p>The x axis is logarithmic in {@code 1/(1-p)}, so each nine of percentile gets equal width. A
 * linear axis spends most of its width on p0 to p90 and squeezes the tail into a few pixels.
 */
final class LatencyChart {

  private static final int WIDTH = 900;
  private static final int HEIGHT = 440;
  private static final int PAD_LEFT = 78;
  private static final int PAD_RIGHT = 150;
  private static final int PAD_TOP = 44;
  private static final int PAD_BOTTOM = 62;

  private static final double[] PERCENTILES = {
    0, 10, 25, 50, 75, 90, 95, 99, 99.5, 99.9, 99.99, 99.999
  };

  /** One nine per label, so a tick answers how deep into the tail it is. */
  private static final double[] AXIS_TICKS = {0, 90, 99, 99.9, 99.99, 99.999};

  private static final String[] SERIES_COLOURS = {
    "#4f9dde", "#e0813d", "#5cb87a", "#b569c9", "#d9534f", "#4dc3c3"
  };

  private LatencyChart() {}

  static String render(List<PhaseResult> results) {
    List<PhaseResult> measured = results.stream().filter(result -> !result.warmup()).toList();
    if (measured.isEmpty()) {
      measured = results;
    }

    double maxMillis =
        measured.stream()
            .mapToDouble(result -> result.latency().getValueAtPercentile(99.999) / 1000d)
            .max()
            .orElse(1d);
    double ceiling = niceCeiling(Math.max(maxMillis, 1d));

    int plotWidth = WIDTH - PAD_LEFT - PAD_RIGHT;
    int plotHeight = HEIGHT - PAD_TOP - PAD_BOTTOM;

    StringBuilder svg = new StringBuilder(8192);
    svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"")
        .append(WIDTH)
        .append("\" height=\"")
        .append(HEIGHT)
        .append("\" viewBox=\"0 0 ")
        .append(WIDTH)
        .append(' ')
        .append(HEIGHT)
        .append(
            "\" font-family=\"ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif\">\n");

    svg.append("<rect width=\"100%\" height=\"100%\" fill=\"#fbfbfd\"/>\n");
    svg.append("<text x=\"")
        .append(PAD_LEFT)
        .append("\" y=\"26\" font-size=\"15\" font-weight=\"600\" fill=\"#1c1c22\">")
        .append("Response-time distribution</text>\n");
    svg.append("<text x=\"")
        .append(PAD_LEFT)
        .append("\" y=\"")
        .append(HEIGHT - 16)
        .append("\" font-size=\"11\" fill=\"#6b6b76\">")
        .append("Percentile (logarithmic in 1/(1-p)), each tick is one more nine</text>\n");

    for (int i = 0; i <= 4; i++) {
      double value = ceiling * i / 4d;
      int y = PAD_TOP + plotHeight - (int) (plotHeight * (i / 4d));
      svg.append("<line x1=\"")
          .append(PAD_LEFT)
          .append("\" y1=\"")
          .append(y)
          .append("\" x2=\"")
          .append(PAD_LEFT + plotWidth)
          .append("\" y2=\"")
          .append(y)
          .append("\" stroke=\"#e3e3ea\" stroke-width=\"1\"/>\n");
      svg.append("<text x=\"")
          .append(PAD_LEFT - 10)
          .append("\" y=\"")
          .append(y + 4)
          .append("\" font-size=\"11\" fill=\"#6b6b76\" text-anchor=\"end\">")
          .append(String.format(Locale.ROOT, "%.0f ms", value))
          .append("</text>\n");
    }

    for (double tick : AXIS_TICKS) {
      int x = PAD_LEFT + (int) (plotWidth * axisPosition(tick));
      svg.append("<line x1=\"")
          .append(x)
          .append("\" y1=\"")
          .append(PAD_TOP)
          .append("\" x2=\"")
          .append(x)
          .append("\" y2=\"")
          .append(PAD_TOP + plotHeight)
          .append("\" stroke=\"#eeeef3\" stroke-width=\"1\"/>\n");
      svg.append("<text x=\"")
          .append(x)
          .append("\" y=\"")
          .append(PAD_TOP + plotHeight + 18)
          .append("\" font-size=\"11\" fill=\"#6b6b76\" text-anchor=\"middle\">")
          .append(tick == 0 ? "p0" : "p" + trim(tick))
          .append("</text>\n");
    }

    int seriesIndex = 0;
    for (PhaseResult result : measured) {
      String colour = SERIES_COLOURS[seriesIndex % SERIES_COLOURS.length];
      StringBuilder path = new StringBuilder();

      for (int i = 0; i < PERCENTILES.length; i++) {
        double percentile = PERCENTILES[i];
        double millis = result.latency().getValueAtPercentile(percentile) / 1000d;
        int x = PAD_LEFT + (int) (plotWidth * axisPosition(percentile));
        int y = PAD_TOP + plotHeight - (int) (plotHeight * Math.min(1d, millis / ceiling));
        path.append(i == 0 ? 'M' : 'L').append(x).append(' ').append(y).append(' ');
      }

      svg.append("<path d=\"")
          .append(path)
          .append("\" fill=\"none\" stroke=\"")
          .append(colour)
          .append("\" stroke-width=\"2\" stroke-linejoin=\"round\"/>\n");

      double p99Millis = result.p99Micros() / 1000d;
      int p99x = PAD_LEFT + (int) (plotWidth * axisPosition(99));
      int p99y = PAD_TOP + plotHeight - (int) (plotHeight * Math.min(1d, p99Millis / ceiling));
      svg.append("<circle cx=\"")
          .append(p99x)
          .append("\" cy=\"")
          .append(p99y)
          .append("\" r=\"3.5\" fill=\"")
          .append(colour)
          .append("\"/>\n");

      int legendY = PAD_TOP + 6 + seriesIndex * 20;
      int legendX = PAD_LEFT + plotWidth + 16;
      svg.append("<rect x=\"")
          .append(legendX)
          .append("\" y=\"")
          .append(legendY - 8)
          .append("\" width=\"11\" height=\"11\" rx=\"2\" fill=\"")
          .append(colour)
          .append("\"/>\n");
      svg.append("<text x=\"")
          .append(legendX + 17)
          .append("\" y=\"")
          .append(legendY + 2)
          .append("\" font-size=\"11\" fill=\"#1c1c22\">")
          .append(escape(result.name()))
          .append("</text>\n");
      svg.append("<text x=\"")
          .append(legendX + 17)
          .append("\" y=\"")
          .append(legendY + 14)
          .append("\" font-size=\"10\" fill=\"#6b6b76\">p99 ")
          .append(String.format(Locale.ROOT, "%.1f ms", p99Millis))
          .append("</text>\n");

      seriesIndex++;
    }

    svg.append("<line x1=\"")
        .append(PAD_LEFT)
        .append("\" y1=\"")
        .append(PAD_TOP + plotHeight)
        .append("\" x2=\"")
        .append(PAD_LEFT + plotWidth)
        .append("\" y2=\"")
        .append(PAD_TOP + plotHeight)
        .append("\" stroke=\"#9a9aa6\" stroke-width=\"1\"/>\n");
    svg.append("<line x1=\"")
        .append(PAD_LEFT)
        .append("\" y1=\"")
        .append(PAD_TOP)
        .append("\" x2=\"")
        .append(PAD_LEFT)
        .append("\" y2=\"")
        .append(PAD_TOP + plotHeight)
        .append("\" stroke=\"#9a9aa6\" stroke-width=\"1\"/>\n");

    svg.append("</svg>\n");
    return svg.toString();
  }

  private static double axisPosition(double percentile) {
    double maxNines = Math.log10(1d / (1d - 99.999 / 100d));
    if (percentile >= 100) {
      return 1d;
    }
    double nines = Math.log10(1d / (1d - percentile / 100d));
    return Math.max(0d, Math.min(1d, nines / maxNines));
  }

  private static double niceCeiling(double value) {
    double magnitude = Math.pow(10, Math.floor(Math.log10(value)));
    double normalised = value / magnitude;
    double rounded = normalised <= 1 ? 1 : normalised <= 2 ? 2 : normalised <= 5 ? 5 : 10;
    return rounded * magnitude;
  }

  private static String trim(double value) {
    return value == Math.floor(value) ? String.valueOf((long) value) : String.valueOf(value);
  }

  private static String escape(String text) {
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }
}
