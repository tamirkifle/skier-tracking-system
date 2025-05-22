package client.common.model;



public class LiftRideEvent {

  private final int skierID;

  private final int resortID;

  private final int liftID;

  private final int seasonID;

  private final int dayID;

  private final int time;

  private final String formattedUrl;



  public LiftRideEvent(int skierID, int resortID, int liftID, int seasonID, int dayID, int time, String baseUrl) {

    this.skierID = skierID;

    this.resortID = resortID;

    this.liftID = liftID;

    this.seasonID = seasonID;

    this.dayID = dayID;

    this.time = time;
    this.formattedUrl = String.format("%s/%d/seasons/%d/days/%d/skier/%d", baseUrl, resortID, seasonID, dayID, skierID);

  }



  public int getSkierID() { return skierID; }

  public int getResortID() { return resortID; }

  public int getLiftID() { return liftID; }

  public int getSeasonID() { return seasonID; }

  public int getDayID() { return dayID; }

  public int getTime() { return time; }

  public String getFormattedUrl() { return formattedUrl; }


  @Override
  public String toString() {
    return "LiftRideEvent{" +
        "skierID=" + skierID +
        ", resortID=" + resortID +
        ", liftID=" + liftID +
        ", seasonID=" + seasonID +
        ", dayID=" + dayID +
        ", time=" + time +
        '}';
  }
}
