package skiers.service;

@FunctionalInterface
public interface FleetRegistry {

  int size();

  /** False until a real count is observed; no seed value is safe for every consumer of it. */
  default boolean isSized() {
    return true;
  }

  static FleetRegistry singleInstance() {
    return () -> 1;
  }

  /** Truncates, and floors at 1 so a divided increase step never reaches zero. */
  static int share(int aggregate, int instances) {
    return Math.max(1, aggregate / Math.max(1, instances));
  }
}
