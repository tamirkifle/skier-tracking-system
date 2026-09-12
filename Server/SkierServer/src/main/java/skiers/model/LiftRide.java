package skiers.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import skiers.Constants;

/** Request body of the lift-ride POST; the bounds drive validation and the generated schema. */
@Schema(description = "A single lift ride: which lift, and at which minute of the ski day.")
public class LiftRide {

  @NotNull
  @Min(Constants.MIN_LIFT_ID)
  @Max(Constants.MAX_LIFT_ID)
  @Schema(description = "Lift identifier", example = "21", minimum = "1", maximum = "40")
  private Integer liftID;

  @NotNull
  @Min(Constants.MIN_TIME)
  @Max(Constants.MAX_TIME)
  @Schema(
      description = "Minute of the ski day the ride started",
      example = "217",
      minimum = "1",
      maximum = "360")
  private Integer time;

  public LiftRide() {}

  public LiftRide(Integer liftID, Integer time) {
    this.liftID = liftID;
    this.time = time;
  }

  public Integer getLiftID() {
    return liftID;
  }

  public void setLiftID(Integer liftID) {
    this.liftID = liftID;
  }

  public Integer getTime() {
    return time;
  }

  public void setTime(Integer time) {
    this.time = time;
  }

  @Override
  public String toString() {
    return "LiftRide{liftID=" + liftID + ", time=" + time + '}';
  }
}
