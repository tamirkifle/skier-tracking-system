package skiers.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import skiers.Constants;
import skiers.ingest.DeliveryAck;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

class WriterTest {

  private static LiftRideEvent event(String skierId, int liftId, int time) {
    return new LiftRideEvent("evt", skierId, "5", "2025", "1", liftId, time, DeliveryAck.NONE);
  }

  @Nested
  @DisplayName("single-put writer")
  class SinglePut {

    private final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);

    @Test
    @DisplayName("writes each item exactly once")
    void writesEachItemOnce() {
      when(dynamoDb.putItem(any(PutItemRequest.class)))
          .thenReturn(PutItemResponse.builder().build());

      SinglePutLiftRideWriter writer = new SinglePutLiftRideWriter(dynamoDb);
      List<LiftRideEvent> failed = writer.write(List.of(event("42", 21, 100)));

      verify(dynamoDb, times(1)).putItem(any(PutItemRequest.class));
      assertThat(failed).isEmpty();
    }

    @Test
    @DisplayName("issues one request per event, each against the LiftRides table")
    void issuesOneRequestPerEvent() {
      when(dynamoDb.putItem(any(PutItemRequest.class)))
          .thenReturn(PutItemResponse.builder().build());

      SinglePutLiftRideWriter writer = new SinglePutLiftRideWriter(dynamoDb);
      writer.write(List.of(event("1", 1, 1), event("2", 2, 2), event("3", 3, 3)));

      ArgumentCaptor<PutItemRequest> puts = ArgumentCaptor.forClass(PutItemRequest.class);
      verify(dynamoDb, times(3)).putItem(puts.capture());
      assertThat(puts.getAllValues())
          .extracting(PutItemRequest::tableName)
          .containsOnly(Constants.LIFT_RIDES_TABLE);
    }

    @Test
    @DisplayName("one failing item does not fail its neighbours")
    void isolatesPerItemFailures() {
      when(dynamoDb.putItem(any(PutItemRequest.class)))
          .thenReturn(PutItemResponse.builder().build())
          .thenThrow(new RuntimeException("throttled"))
          .thenReturn(PutItemResponse.builder().build());

      SinglePutLiftRideWriter writer = new SinglePutLiftRideWriter(dynamoDb);
      LiftRideEvent second = event("2", 2, 2);
      List<LiftRideEvent> failed =
          writer.write(List.of(event("1", 1, 1), second, event("3", 3, 3)));

      assertThat(failed).containsExactly(second);
    }
  }
}
