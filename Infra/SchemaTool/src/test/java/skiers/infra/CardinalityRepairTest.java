package skiers.infra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class CardinalityRepairTest {

  private DynamoDbClient client;
  private CardinalityRepair repair;

  @BeforeEach
  void setUp() {
    client = mock(DynamoDbClient.class);
    repair = new CardinalityRepair(client);
  }

  private static Map<String, AttributeValue> sentinel(String resortSeasonDay) {
    return Map.of(DynamoDbSchema.RESORT_SEASON_DAY, AttributeValue.fromS(resortSeasonDay));
  }

  private void sentinels(Map<String, AttributeValue>... items) {
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(items).build());
  }

  private void storedCount(long count) {
    when(client.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder()
                .item(
                    Map.of(
                        DynamoDbSchema.RESORT_SEASON_DAY,
                        AttributeValue.fromS("5#2025#1"),
                        "uniqueSkierCount",
                        AttributeValue.fromN(Long.toString(count))))
                .build());
  }

  @Test
  @DisplayName("a count short of its sentinels is repaired, conditional on not having moved")
  void repairsAnUndercount() {
    sentinels(sentinel("5#2025#1"), sentinel("5#2025#1"), sentinel("5#2025#1"));
    storedCount(2);
    when(client.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().build());

    assertThat(repair.apply(repair.recount())).isEqualTo(1);

    ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(update.capture());
    assertThat(update.getValue().updateExpression()).isEqualTo("SET uniqueSkierCount = :recounted");
    assertThat(update.getValue().expressionAttributeValues().get(":recounted").n()).isEqualTo("3");
    assertThat(update.getValue().conditionExpression()).isEqualTo("uniqueSkierCount = :stored");
  }

  @Test
  @DisplayName("a recount below the stored value is skipped, because that is expired sentinels")
  void refusesToLowerACount() {
    sentinels(sentinel("5#2025#1"));
    storedCount(9000);

    assertThat(repair.recount().get("5#2025#1").needsRepair()).isFalse();
    assertThat(repair.apply(repair.recount())).isZero();
    verify(client, never()).updateItem(any(UpdateItemRequest.class));
  }

  @Test
  @DisplayName("a row that changed under the scan is reported, not clobbered")
  void reportsALostRace() {
    sentinels(sentinel("5#2025#1"), sentinel("5#2025#1"));
    storedCount(1);
    when(client.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(ConditionalCheckFailedException.builder().message("moved").build());

    assertThat(repair.apply(repair.recount())).isZero();
  }

  @Test
  @DisplayName("a sentinel count that agrees with the row is left alone")
  void leavesAnAgreeingCountAlone() {
    sentinels(sentinel("5#2025#1"), sentinel("5#2025#1"));
    storedCount(2);

    CardinalityRepair.Finding finding = repair.recount().get("5#2025#1");
    assertThat(finding.needsRepair()).isFalse();
    assertThat(finding.shortfall()).isZero();
  }

  @Test
  @DisplayName("sentinels are counted per resort-day, not summed across the table")
  void groupsByResortDay() {
    sentinels(sentinel("5#2025#1"), sentinel("7#2025#1"), sentinel("5#2025#1"));
    storedCount(0);

    Map<String, CardinalityRepair.Finding> findings = repair.recount();
    assertThat(findings.get("5#2025#1").recounted()).isEqualTo(2);
    assertThat(findings.get("7#2025#1").recounted()).isEqualTo(1);
  }
}
