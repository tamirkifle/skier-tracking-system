package client.common.model;

/** One lift-ride event to be POSTed. The URL and JSON body are precomputed off the hot path. */
public final class LiftRideEvent {

  private final int skierID;
  private final int resortID;
  private final int liftID;
  private final int seasonID;
  private final int dayID;
  private final int time;
  private final String formattedUrl;
  private final String jsonBody;

  public LiftRideEvent(
      int skierID, int resortID, int liftID, int seasonID, int dayID, int time, String baseUrl) {
    this.skierID = skierID;
    this.resortID = resortID;
    this.liftID = liftID;
    this.seasonID = seasonID;
    this.dayID = dayID;
    this.time = time;
    this.formattedUrl =
        String.format(
            "%s/%d/seasons/%d/days/%d/skier/%d", baseUrl, resortID, seasonID, dayID, skierID);
    this.jsonBody = "{\"time\":" + time + ",\"liftID\":" + liftID + "}";
  }

  public int getSkierID() {
    return skierID;
  }

  public int getResortID() {
    return resortID;
  }

  public int getLiftID() {
    return liftID;
  }

  public int getSeasonID() {
    return seasonID;
  }

  public int getDayID() {
    return dayID;
  }

  public int getTime() {
    return time;
  }

  public String getFormattedUrl() {
    return formattedUrl;
  }

  public String jsonBody() {
    return jsonBody;
  }

  @Override
  public String toString() {
    return "LiftRideEvent{skier="
        + skierID
        + ", resort="
        + resortID
        + ", lift="
        + liftID
        + ", season="
        + seasonID
        + ", day="
        + dayID
        + ", time="
        + time
        + '}';
  }
}
