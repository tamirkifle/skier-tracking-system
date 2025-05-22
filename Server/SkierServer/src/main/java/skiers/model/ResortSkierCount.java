package skiers.model;

import java.io.Serializable;

public class ResortSkierCount implements Serializable {
  private String resortID;
  private int uniqueNumSkiers;

  // Default constructor
  public ResortSkierCount() {}

  // Constructor
  public ResortSkierCount(String resortID, int uniqueNumSkiers) {
    this.resortID = resortID;
    this.uniqueNumSkiers = uniqueNumSkiers;
  }

  // Getters and Setters
  public String getResortID() { return resortID; }
  public void setResortID(String resortID) { this.resortID = resortID; }

  public int getUniqueNumSkiers() { return uniqueNumSkiers; }
  public void setUniqueNumSkiers(int uniqueNumSkiers) { this.uniqueNumSkiers = uniqueNumSkiers; }
}
