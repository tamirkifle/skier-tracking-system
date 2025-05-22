package skiers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SkierDataStore {
  private final SkierDataProcessor dataProcessor;

  @Autowired
  public SkierDataStore(SkierDataProcessor dataProcessor) {
    this.dataProcessor = dataProcessor;
  }

  public void addSkierEvent(String skierId, skiers.model.LiftRide event, String resortId, String seasonId, String dayId) {
    dataProcessor.addSkierEvent(skierId, event, resortId, seasonId, dayId);
  }
}