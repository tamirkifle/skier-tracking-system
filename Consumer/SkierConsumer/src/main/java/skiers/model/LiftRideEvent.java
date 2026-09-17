package skiers.model;

import java.util.Objects;
import skiers.Constants;
import skiers.ingest.DeliveryAck;

/** One lift ride in flight through the write path, with the right to settle its delivery. */
public final class LiftRideEvent {

  private final String eventId;
  private final String skierId;
  private final String resortId;
  private final String seasonId;
  private final String dayId;
  private final int liftId;
  private final int time;
  private final DeliveryAck ack;

  private final Long publishedAtMillis;

  private int attempts;

  public LiftRideEvent(
      String eventId,
      String skierId,
      String resortId,
      String seasonId,
      String dayId,
      int liftId,
      int time,
      DeliveryAck ack) {
    this(eventId, skierId, resortId, seasonId, dayId, liftId, time, ack, null);
  }

  public LiftRideEvent(
      String eventId,
      String skierId,
      String resortId,
      String seasonId,
      String dayId,
      int liftId,
      int time,
      DeliveryAck ack,
      Long publishedAtMillis) {
    this(eventId, skierId, resortId, seasonId, dayId, liftId, time, ack, publishedAtMillis, 0);
  }

  public LiftRideEvent(
      String eventId,
      String skierId,
      String resortId,
      String seasonId,
      String dayId,
      int liftId,
      int time,
      DeliveryAck ack,
      Long publishedAtMillis,
      int priorAttempts) {
    this.attempts = priorAttempts;
    this.publishedAtMillis = publishedAtMillis;
    this.eventId = eventId;
    this.skierId = Objects.requireNonNull(skierId, "skierId");
    this.resortId = Objects.requireNonNull(resortId, "resortId");
    this.seasonId = Objects.requireNonNull(seasonId, "seasonId");
    this.dayId = Objects.requireNonNull(dayId, "dayId");
    this.liftId = liftId;
    this.time = time;
    this.ack = ack == null ? DeliveryAck.NONE : ack;
  }

  public String eventId() {
    return eventId;
  }

  public Long publishedAtMillis() {
    return publishedAtMillis;
  }

  public String skierId() {
    return skierId;
  }

  public String resortId() {
    return resortId;
  }

  public String seasonId() {
    return seasonId;
  }

  public String dayId() {
    return dayId;
  }

  public int liftId() {
    return liftId;
  }

  public int time() {
    return time;
  }

  public DeliveryAck ack() {
    return ack;
  }

  public int attempts() {
    return attempts;
  }

  public int recordAttempt() {
    return ++attempts;
  }

  public int vertical() {
    return liftId * Constants.VERTICAL_FEET_PER_LIFT;
  }

  /** Every coordinate of the ride, so a redelivery overwrites and a different lift does not. */
  public String sortKey() {
    return resortId + '#' + seasonId + '#' + dayId + '#' + time + '#' + liftId;
  }

  public String skierSeasonKey() {
    return skierId + '#' + seasonId;
  }

  public String resortDayKey() {
    return resortId + '#' + dayId;
  }

  public String resortSkierKey() {
    return resortId + '#' + skierId;
  }

  public String seasonDayKey() {
    return seasonId + '#' + dayId;
  }

  public String resortSeasonDayKey() {
    return resortId + '#' + seasonId + '#' + dayId;
  }

  public String skierDayIdentity() {
    return resortSeasonDayKey() + '#' + skierId;
  }

  @Override
  public String toString() {
    return "LiftRideEvent{skier="
        + skierId
        + ", resort="
        + resortId
        + ", season="
        + seasonId
        + ", day="
        + dayId
        + ", lift="
        + liftId
        + ", time="
        + time
        + ", attempts="
        + attempts
        + '}';
  }
}
