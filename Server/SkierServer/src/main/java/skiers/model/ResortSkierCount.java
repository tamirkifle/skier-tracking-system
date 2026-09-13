package skiers.model;

import java.io.Serializable;

public class ResortSkierCount implements Serializable {
  private String resortID;
  private int uniqueNumSkiers;

  public ResortSkierCount() {}

  public ResortSkierCount(String resortID, int uniqueNumSkiers) {
    this.resortID = resortID;
    this.uniqueNumSkiers = uniqueNumSkiers;
  }

  public String getResortID() {
    return resortID;
  }

  public void setResortID(String resortID) {
    this.resortID = resortID;
  }

  public int getUniqueNumSkiers() {
    return uniqueNumSkiers;
  }

  public void setUniqueNumSkiers(int uniqueNumSkiers) {
    this.uniqueNumSkiers = uniqueNumSkiers;
  }
}
