package skiers.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import skiers.ingest.DeliveryAck;

class LiftRideEventTest {

  private static LiftRideEvent event(
      String skier, String resort, String season, String day, int lift, int time) {
    return new LiftRideEvent("evt", skier, resort, season, day, lift, time, DeliveryAck.NONE);
  }

  @Test
  @DisplayName("vertical is liftID x 10")
  void computesVertical() {
    assertThat(event("42", "5", "2025", "1", 21, 100).vertical()).isEqualTo(210);
    assertThat(event("42", "5", "2025", "1", 1, 100).vertical()).isEqualTo(10);
    assertThat(event("42", "5", "2025", "1", 40, 100).vertical()).isEqualTo(400);
  }

  @Test
  @DisplayName("composite keys match the schema documented for each index")
  void composesEveryIndexKey() {
    LiftRideEvent event = event("42", "5", "2025", "1", 21, 217);

    assertThat(event.sortKey()).isEqualTo("5#2025#1#217");
    assertThat(event.skierSeasonKey()).isEqualTo("42#2025");
    assertThat(event.resortDayKey()).isEqualTo("5#1");
    assertThat(event.resortSkierKey()).isEqualTo("5#42");
    assertThat(event.seasonDayKey()).isEqualTo("2025#1");
    assertThat(event.resortSeasonDayKey()).isEqualTo("5#2025#1");
    assertThat(event.skierDayIdentity()).isEqualTo("5#2025#1#42");
  }

  @Test
  @DisplayName("the sort key keys on minute-of-day, so same-minute rides collide")
  void sameMinuteRidesShareAPrimaryKey() {
    LiftRideEvent first = event("42", "5", "2025", "1", 21, 100);
    LiftRideEvent second = event("42", "5", "2025", "1", 7, 100);

    assertThat(first.sortKey()).isEqualTo(second.sortKey());
    assertThat(first.vertical()).isNotEqualTo(second.vertical());
  }

  @Test
  @DisplayName("retry attempts accumulate per event")
  void tracksAttempts() {
    LiftRideEvent event = event("42", "5", "2025", "1", 21, 100);

    assertThat(event.attempts()).isZero();
    assertThat(event.recordAttempt()).isEqualTo(1);
    assertThat(event.recordAttempt()).isEqualTo(2);
    assertThat(event.attempts()).isEqualTo(2);
  }

  @Test
  @DisplayName("a missing key component is rejected at construction")
  void rejectsNullKeyComponents() {
    assertThatThrownBy(() -> event(null, "5", "2025", "1", 21, 100))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("skierId");
    assertThatThrownBy(() -> event("42", null, "2025", "1", 21, 100))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("resortId");
  }

  @Test
  @DisplayName("a missing ack becomes the inert one rather than a null")
  void defaultsToInertAck() {
    LiftRideEvent event = new LiftRideEvent("evt", "42", "5", "2025", "1", 21, 100, null);
    assertThat(event.ack()).isSameAs(DeliveryAck.NONE);
  }

  @Test
  @DisplayName("distinct deliveries of the same ride remain distinct objects")
  void doesNotCollapseByValue() {
    LiftRideEvent first = event("42", "5", "2025", "1", 21, 100);
    LiftRideEvent second = event("42", "5", "2025", "1", 21, 100);

    assertThat(first).isNotEqualTo(second);
  }
}
