package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import skiers.Constants;
import skiers.model.SkierVertical;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

class SkierServiceTest {

  private DynamoDbClient dynamoDb;
  private SkierService service;

  @BeforeEach
  void setUp() {
    dynamoDb = mock(DynamoDbClient.class);
    service = new SkierService(dynamoDb);
  }

  private static Map<String, AttributeValue> rideItem(String vertical, String seasonDay) {
    return Map.of(
        Constants.ATTR_VERTICAL, AttributeValue.fromS(vertical),
        Constants.ATTR_SEASON_DAY, AttributeValue.fromS(seasonDay));
  }

  private static Map<String, AttributeValue> numericRideItem(String vertical, String seasonDay) {
    return Map.of(
        Constants.ATTR_VERTICAL, AttributeValue.fromN(vertical),
        Constants.ATTR_SEASON_DAY, AttributeValue.fromS(seasonDay));
  }

  /** A page with no continuation. v2 signals the last page with an empty key, not a null one. */
  @SafeVarargs
  private static QueryResponse lastPage(Map<String, AttributeValue>... items) {
    return QueryResponse.builder().items(items).lastEvaluatedKey(Map.of()).build();
  }

  private static QueryResponse pageFollowedByMore(Map<String, AttributeValue> item) {
    return QueryResponse.builder()
        .items(item)
        .lastEvaluatedKey(Map.of(Constants.ATTR_SKIER_ID, AttributeValue.fromS("42")))
        .build();
  }

  @Test
  @DisplayName("day vertical sums every ride in the resort-day range")
  void sumsDayVertical() {
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(lastPage(rideItem("100", "2025#1"), rideItem("250", "2025#1")));

    SkierVertical result = service.getSkierData("5", "2025", "1", "42");

    assertThat(result.getTotalVertical()).isEqualTo(350);
    assertThat(result.getSkierID()).isEqualTo("42");
  }

  @Test
  @DisplayName("day vertical follows LastEvaluatedKey across every page")
  void paginatesDayVertical() {
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(pageFollowedByMore(rideItem("100", "2025#1")))
        .thenReturn(pageFollowedByMore(rideItem("200", "2025#1")))
        .thenReturn(lastPage(rideItem("300", "2025#1")));

    SkierVertical result = service.getSkierData("5", "2025", "1", "42");

    assertThat(result.getTotalVertical()).isEqualTo(600);
    verify(dynamoDb, times(3)).query(any(QueryRequest.class));
  }

  @Test
  @DisplayName("each page carries the previous page's cursor as its exclusive start key")
  void carriesTheCursorOntoTheNextPage() {
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(pageFollowedByMore(rideItem("100", "2025#1")))
        .thenReturn(lastPage(rideItem("200", "2025#1")));

    service.getSkierData("5", "2025", "1", "42");

    ArgumentCaptor<QueryRequest> pages = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDb, times(2)).query(pages.capture());
    assertThat(pages.getAllValues().get(0).exclusiveStartKey()).isEmpty();
    assertThat(pages.getAllValues().get(1).exclusiveStartKey())
        .containsEntry(Constants.ATTR_SKIER_ID, AttributeValue.fromS("42"));
    assertThat(pages.getAllValues().get(1).keyConditionExpression())
        .isEqualTo(pages.getAllValues().get(0).keyConditionExpression());
  }

  @Test
  @DisplayName("vertical stored as a DynamoDB number parses identically to a string")
  void acceptsBothAttributeTypes() {
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(lastPage(numericRideItem("100", "2025#1"), rideItem("50", "2025#1")));

    assertThat(service.getSkierData("5", "2025", "1", "42").getTotalVertical()).isEqualTo(150);
  }

  @Test
  @DisplayName("a non-numeric attribute is ignored rather than throwing")
  void tolerateCorruptAttribute() {
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(lastPage(rideItem("not-a-number", "2025#1"), rideItem("100", "2025#1")));

    assertThat(service.getSkierData("5", "2025", "1", "42").getTotalVertical()).isEqualTo(100);
  }
}
