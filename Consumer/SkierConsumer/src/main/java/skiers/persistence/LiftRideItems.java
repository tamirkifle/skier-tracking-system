package skiers.persistence;

import java.util.HashMap;
import java.util.Map;
import skiers.Constants;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** The single projection of an event into a LiftRides item, materialised GSI keys included. */
public final class LiftRideItems {

  private LiftRideItems() {}

  /** liftID and vertical are strings; a number stored as a string sorts lexically. */
  public static Map<String, AttributeValue> toAttributeMap(LiftRideEvent event) {
    Map<String, AttributeValue> item = new HashMap<>(16);
    item.put(Constants.ATTR_SKIER_ID, str(event.skierId()));
    item.put(Constants.ATTR_SORT_KEY, str(event.sortKey()));
    item.put(Constants.ATTR_RESORT_ID, str(event.resortId()));
    item.put(Constants.ATTR_DAY_ID, str(event.dayId()));
    item.put(Constants.ATTR_LIFT_ID, str(String.valueOf(event.liftId())));
    item.put(Constants.ATTR_VERTICAL, str(String.valueOf(event.vertical())));
    item.put(Constants.ATTR_SKIER_SEASON, str(event.skierSeasonKey()));
    item.put(Constants.ATTR_RESORT_DAY, str(event.resortDayKey()));
    item.put(Constants.ATTR_RESORT_SKIER, str(event.resortSkierKey()));
    item.put(Constants.ATTR_SEASON_DAY, str(event.seasonDayKey()));
    item.put(Constants.ATTR_RESORT_SEASON_DAY, str(event.resortSeasonDayKey()));
    return item;
  }

  private static AttributeValue str(String value) {
    return AttributeValue.builder().s(value).build();
  }
}
