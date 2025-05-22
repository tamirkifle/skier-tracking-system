package client.clientpart2;

import client.clientpart2.LatencyRecorder.LatencyRecord;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCalculator {
  private static final Logger log = LoggerFactory.getLogger(MetricsCalculator.class);
  private static final String CSV_DIR = "src/main/resources/reports";
  private static final String CSV_PATH = CSV_DIR + "/latencies.csv";

  public static void calculateAndPrintMetrics(List<LatencyRecord> latencies) {
    List<Long> sortedLatencies = latencies.stream()
        .map(LatencyRecord::getLatency)
        .sorted()
        .collect(Collectors.toList());

    if (sortedLatencies.isEmpty()) {
      log.warn("No latency data available");
      return;
    }

    long sum = sortedLatencies.stream().mapToLong(Long::longValue).sum();
    double mean = sum / (double) sortedLatencies.size();

    // Fixed median calculation for even/odd sizes
    int midIndex = sortedLatencies.size() / 2;
    long median;
    if (sortedLatencies.size() % 2 == 0) {
      median = (sortedLatencies.get(midIndex - 1) + sortedLatencies.get(midIndex)) / 2;
    } else {
      median = sortedLatencies.get(midIndex);
    }

    // Improved p99 index calculation
    int p99Index = (int) Math.ceil(sortedLatencies.size() * 0.99) - 1;
    long p99 = sortedLatencies.get(Math.min(p99Index, sortedLatencies.size() - 1));

    long min = sortedLatencies.get(0);
    long max = sortedLatencies.get(sortedLatencies.size() - 1);

    log.info("\n=== Performance Metrics ===");
    log.info("Mean response time: {} ms", String.format("%.2f", mean));
    log.info("Median response time: {} ms", median);
    log.info("99th percentile response time: {} ms", p99);
    log.info("Min response time: {} ms", min);
    log.info("Max response time: {} ms", max);

    exportToCSV(latencies);
  }

  private static void exportToCSV(List<LatencyRecord> records) {
    try {
      File directory = new File(CSV_DIR);
      if (!directory.exists()) {
        directory.mkdirs();
      }

      try (FileWriter writer = new FileWriter(CSV_PATH)) {
        writer.write("start_time,request_type,latency_ms,response_code\n");
        for (LatencyRecord record : records) {
          writer.write(String.format("%d,%s,%d,%d%n",
              record.getStartTime(),
              record.getRequestType(),
              record.getLatency(),
              record.getResponseCode()));
        }
      }

      log.info("\nCSV report saved to: {}", Paths.get(CSV_PATH).toAbsolutePath());

    } catch (IOException e) {
      log.error("Failed to write CSV file: {}", e.getMessage(), e);
    }
  }
}