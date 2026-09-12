package skiers.infra;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

class DynamoDbSchemaTest {

  private static Map<String, CreateTableRequest> tables() {
    return DynamoDbSchema.tables().stream()
        .collect(Collectors.toMap(CreateTableRequest::tableName, Function.identity()));
  }

  private static Map<KeyType, String> keys(List<KeySchemaElement> schema) {
    return schema.stream()
        .collect(Collectors.toMap(KeySchemaElement::keyType, KeySchemaElement::attributeName));
  }

  private static GlobalSecondaryIndex index(String name) {
    return tables().get(DynamoDbSchema.LIFT_RIDES).globalSecondaryIndexes().stream()
        .filter(gsi -> gsi.indexName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  @Test
  @DisplayName("all three tables are declared, and nothing else is")
  void declaresThreeTables() {
    assertThat(tables().keySet())
        .containsExactlyInAnyOrder(
            DynamoDbSchema.LIFT_RIDES, DynamoDbSchema.SKIER_COUNTS, DynamoDbSchema.SKIER_TRACKING);
  }

  @Test
  @DisplayName("LiftRides is partitioned by skier, not by resort")
  void liftRidesIsPartitionedBySkier() {
    Map<KeyType, String> key = keys(tables().get(DynamoDbSchema.LIFT_RIDES).keySchema());

    // DynamoDB caps a partition at 1,000 WCU, so a resort key would cap the table near 10,000 w/s.
    assertThat(key.get(KeyType.HASH)).isEqualTo(DynamoDbSchema.SKIER_ID);
    assertThat(key.get(KeyType.RANGE)).isEqualTo(DynamoDbSchema.SORT_KEY);
  }

  @Test
  @DisplayName("CS-Index is partitioned by resort#skier, not resort#season")
  void csIndexIncludesTheSkierInItsPartitionKey() {
    Map<KeyType, String> key = keys(index(DynamoDbSchema.CS_INDEX).keySchema());

    assertThat(key.get(KeyType.HASH)).isEqualTo(DynamoDbSchema.RESORT_SKIER);
    assertThat(key.get(KeyType.RANGE)).isEqualTo(DynamoDbSchema.SEASON_DAY);
  }

  @Test
  @DisplayName("no index projects ALL, because a projection is paid for on every write")
  void noIndexProjectsEverything() {
    assertThat(tables().get(DynamoDbSchema.LIFT_RIDES).globalSecondaryIndexes())
        .allSatisfy(
            gsi -> assertThat(gsi.projection().projectionType()).isNotEqualTo(ProjectionType.ALL));

    assertThat(index(DynamoDbSchema.RD_INDEX).projection().projectionType())
        .isEqualTo(ProjectionType.KEYS_ONLY);
  }

  @Test
  @DisplayName("every key attribute an index uses is declared on the table")
  void indexKeyAttributesAreDeclared() {
    CreateTableRequest liftRides = tables().get(DynamoDbSchema.LIFT_RIDES);
    List<String> declared =
        liftRides.attributeDefinitions().stream()
            .map(definition -> definition.attributeName())
            .toList();

    for (GlobalSecondaryIndex gsi : liftRides.globalSecondaryIndexes()) {
      for (KeySchemaElement element : gsi.keySchema()) {
        assertThat(declared)
            .as("%s keys on %s", gsi.indexName(), element.attributeName())
            .contains(element.attributeName());
      }
    }
  }

  @Test
  @DisplayName("SkierTracking is keyed by the sentinel identity alone")
  void skierTrackingIsKeyedBySentinel() {
    Map<KeyType, String> key = keys(tables().get(DynamoDbSchema.SKIER_TRACKING).keySchema());

    assertThat(key.get(KeyType.HASH)).isEqualTo(DynamoDbSchema.SKIER_KEY);
    assertThat(key).doesNotContainKey(KeyType.RANGE);
  }
}
