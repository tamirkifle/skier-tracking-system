package client.clientpart2;



import java.util.ArrayList;

import java.util.Collections;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;



public class LatencyRecorder {

  private final List<LatencyRecord> latencies = Collections.synchronizedList(new ArrayList<>());

  private final AtomicInteger successfulRequests = new AtomicInteger(0);

  private final AtomicInteger failedRequests = new AtomicInteger(0);



  public synchronized void recordLatency(long startTime, String requestType, long latency, int responseCode) {


    if (responseCode >= 200 && responseCode < 300) {

      successfulRequests.incrementAndGet();

    } else {

      failedRequests.incrementAndGet();

    }
    latencies.add(new LatencyRecord(startTime, requestType, latency, responseCode));

  }



  public List<LatencyRecord> getLatencies() {

    return latencies;

  }



  public int getSuccessfulRequests() {

    return successfulRequests.get();

  }



  public int getFailedRequests() {

    return failedRequests.get();

  }



  public static class LatencyRecord {

    private final long startTime;

    private final String requestType;

    private final long latency;

    private final int responseCode;



    public LatencyRecord(long startTime, String requestType, long latency, int responseCode) {

      this.startTime = startTime;

      this.requestType = requestType;

      this.latency = latency;

      this.responseCode = responseCode;

    }



    public long getStartTime() { return startTime; }

    public String getRequestType() { return requestType; }

    public long getLatency() { return latency; }

    public int getResponseCode() { return responseCode; }

  }

}
