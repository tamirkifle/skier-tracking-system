package client.engine;

import client.generator.EventFactory;
import client.metrics.LatencyRecorder;
import client.scenario.Scenario;
import client.transport.HttpTransport;

/** Drives one phase of a scenario. */
public interface LoadEngine {

  Dispatch run(
      Scenario.Phase phase,
      EventFactory events,
      HttpTransport transport,
      LatencyRecorder recorder,
      ProgressSink progress)
      throws InterruptedException;

  record Dispatch(
      long issued, double dispatchSeconds, long lateDispatches, long maxLatenessMillis) {

    static Dispatch onDemand(long issued, double dispatchSeconds) {
      return new Dispatch(issued, dispatchSeconds, 0, 0);
    }
  }

  interface ProgressSink {
    void sampleConcurrency(int inFlight);

    ProgressSink NONE = inFlight -> {};
  }
}
