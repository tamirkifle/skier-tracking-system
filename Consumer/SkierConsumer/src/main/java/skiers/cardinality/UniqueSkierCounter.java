package skiers.cardinality;

import skiers.model.LiftRideEvent;

public interface UniqueSkierCounter {

  boolean observe(LiftRideEvent event);

  default boolean projectionRequired() {
    return false;
  }

  String name();
}
