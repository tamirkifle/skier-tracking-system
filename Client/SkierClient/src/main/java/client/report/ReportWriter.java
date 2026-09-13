package client.report;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReportWriter {

  private static final Logger log = LoggerFactory.getLogger(ReportWriter.class);

  private static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneOffset.UTC);

  private final Path outputDir;

  public ReportWriter(Path outputDir) {
    this.outputDir = outputDir;
  }

  private void writeHistogram(PhaseResult result) throws IOException {
    Path path = outputDir.resolve("latency-" + slug(result.name()) + ".hgrm");
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192);
    try (PrintStream printer = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
      result.latency().outputPercentileDistribution(printer, 1000.0);
    }
    Files.write(path, buffer.toByteArray());
  }

  static String slug(String value) {
    return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "");
  }
}
