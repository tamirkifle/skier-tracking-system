package skiers.persistence;

import java.util.List;
import skiers.model.LiftRideEvent;

public interface LiftRideWriter {

  /** Writes a group and returns the events that must be retried; empty on full success. */
  List<LiftRideEvent> write(List<LiftRideEvent> events) throws Exception;

  String name();

  int maxBatchSize();
}
