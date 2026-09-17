package skiers.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import skiers.Constants;
import skiers.ingest.DeliveryAck;
import skiers.model.LiftRideEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

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

  @Nested
  @DisplayName("batching writer")
  class Batching {

    private final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);

    private void respondWith(BatchWriteItemResponse response) {
      when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
          .thenReturn(CompletableFuture.completedFuture(response));
    }

    @Test
    @DisplayName("coalesces every event into one request")
    void coalescesIntoOneRequest() throws Exception {
      respondWith(BatchWriteItemResponse.builder().build());
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);

      List<LiftRideEvent> events = List.of(event("1", 1, 1), event("2", 2, 2), event("3", 3, 3));
      assertThat(writer.write(events)).isEmpty();

      ArgumentCaptor<BatchWriteItemRequest> captor =
          ArgumentCaptor.forClass(BatchWriteItemRequest.class);
      verify(client, times(1)).batchWriteItem(captor.capture());
      assertThat(captor.getValue().requestItems().get(Constants.LIFT_RIDES_TABLE)).hasSize(3);
    }

    @Test
    @DisplayName("batch size is clamped to the API's hard limit of 25")
    void clampsToApiLimit() {
      assertThat(new BatchingLiftRideWriter(client, 500).maxBatchSize())
          .isEqualTo(Constants.MAX_BATCH_WRITE_ITEMS);
      assertThat(new BatchingLiftRideWriter(client, 0).maxBatchSize()).isEqualTo(1);
    }

    @Test
    @DisplayName("unprocessed items are returned for retry, not reported as written")
    void returnsUnprocessedItems() throws Exception {
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);
      LiftRideEvent throttled = event("2", 2, 2);
      Map<String, AttributeValue> throttledItem = LiftRideItems.toAttributeMap(throttled);

      respondWith(
          BatchWriteItemResponse.builder()
              .unprocessedItems(
                  Map.of(
                      Constants.LIFT_RIDES_TABLE,
                      List.of(
                          WriteRequest.builder()
                              .putRequest(PutRequest.builder().item(throttledItem).build())
                              .build())))
              .build());

      List<LiftRideEvent> deferred =
          writer.write(List.of(event("1", 1, 1), throttled, event("3", 3, 3)));

      // BatchWriteItem answers 200 with UnprocessedItems when individual items throttle.
      assertThat(deferred).containsExactly(throttled);
    }

    @Test
    @DisplayName("two lifts in the same minute are two items in one request, not a collision")
    void writesEveryLiftRiddenInAMinute() throws Exception {
      respondWith(BatchWriteItemResponse.builder().build());
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);

      // The regression the lift in the sort key exists for. These two once shared a primary key,
      // so the second was deferred to a later request and then overwrote the first.
      LiftRideEvent first = event("42", 21, 100);
      LiftRideEvent sameMinute = event("42", 7, 100);
      assertThat(first.sortKey()).isNotEqualTo(sameMinute.sortKey());

      List<LiftRideEvent> deferred = writer.write(List.of(first, sameMinute));

      assertThat(deferred).isEmpty();
      ArgumentCaptor<BatchWriteItemRequest> captor =
          ArgumentCaptor.forClass(BatchWriteItemRequest.class);
      verify(client).batchWriteItem(captor.capture());
      assertThat(captor.getValue().requestItems().get(Constants.LIFT_RIDES_TABLE)).hasSize(2);
    }

    @Test
    @DisplayName("N redeliveries of one event write one item and defer the rest")
    void deduplicatesRepeatedDeliveries() throws Exception {
      respondWith(BatchWriteItemResponse.builder().build());
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);
      LiftRideEvent event = event("42", 21, 100);

      assertThat(writer.write(List.of(event, event, event))).hasSize(2);

      ArgumentCaptor<BatchWriteItemRequest> captor =
          ArgumentCaptor.forClass(BatchWriteItemRequest.class);
      verify(client, times(1)).batchWriteItem(captor.capture());
      assertThat(captor.getValue().requestItems().get(Constants.LIFT_RIDES_TABLE)).hasSize(1);
    }

    @Test
    @DisplayName("an empty group is a no-op")
    void emptyGroupIsNoOp() throws Exception {
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);
      assertThat(writer.write(List.of())).isEmpty();
      verify(client, times(0)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    @DisplayName("a request-level failure surfaces the SDK's own exception, not a wrapper")
    void unwrapsAsyncFailures() {
      BatchingLiftRideWriter writer = new BatchingLiftRideWriter(client, 25);
      when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
          .thenReturn(
              CompletableFuture.failedFuture(
                  ProvisionedThroughputExceededException.builder().message("slow down").build()));

      assertThatThrownBy(() -> writer.write(List.of(event("1", 1, 1))))
          .isInstanceOf(ProvisionedThroughputExceededException.class);
    }

    @Test
    @DisplayName("the shared item projection writes exactly the schema's attributes")
    void itemProjectionMatchesTheSchema() {
      Map<String, AttributeValue> attributes = LiftRideItems.toAttributeMap(event("42", 21, 217));

      assertThat(attributes.keySet())
          .containsExactlyInAnyOrder(
              Constants.ATTR_SKIER_ID,
              Constants.ATTR_SORT_KEY,
              Constants.ATTR_RESORT_ID,
              Constants.ATTR_DAY_ID,
              Constants.ATTR_LIFT_ID,
              Constants.ATTR_VERTICAL,
              Constants.ATTR_SKIER_SEASON,
              Constants.ATTR_RESORT_DAY,
              Constants.ATTR_RESORT_SKIER,
              Constants.ATTR_SEASON_DAY,
              Constants.ATTR_RESORT_SEASON_DAY);
      assertThat(attributes.get(Constants.ATTR_VERTICAL).s()).isEqualTo("210");
      assertThat(attributes.get(Constants.ATTR_SORT_KEY).s()).isEqualTo("5#2025#1#217#21");
    }
  }
}
