package skiers.model;

import java.io.Serializable;

public class SkierVertical implements Serializable {
  private String resortID;
  private String seasonID;
  private String dayID;
  private String skierID;
  private int totalVertical;

  // Default constructor
  public SkierVertical() {}

  // Constructor
  public SkierVertical(String resortID, String seasonID, String dayID, String skierID, int totalVertical) {
    this.resortID = resortID;
    this.seasonID = seasonID;
    this.dayID = dayID;
    this.skierID = skierID;
    this.totalVertical = totalVertical;
  }

  // Getters and Setters
  public String getResortID() { return resortID; }
  public void setResortID(String resortID) { this.resortID = resortID; }

  public String getSeasonID() { return seasonID; }
  public void setSeasonID(String seasonID) { this.seasonID = seasonID; }

  public String getDayID() { return dayID; }
  public void setDayID(String dayID) { this.dayID = dayID; }

  public String getSkierID() { return skierID; }
  public void setSkierID(String skierID) { this.skierID = skierID; }

  public int getTotalVertical() { return totalVertical; }
  public void setTotalVertical(int totalVertical) { this.totalVertical = totalVertical; }
}