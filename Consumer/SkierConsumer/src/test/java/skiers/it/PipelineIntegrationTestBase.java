package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import skiers.Constants;
import skiers.SkierDataProcessor;
import skiers.cardinality.UniqueSkierCounter;
import skiers.infra.SchemaBootstrap;
import skiers.persistence.LiftRideWriter;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

// Two contracts every subclass must honour, or its suite passes while testing nothing.
// 1. Register the strategy property AND assert the bean type received. Unset picks the default,
//    and setting it alone can still test the other arm: WritePathConfig falls back with a warning.
// 2. Scope every assertion to the run that makes it, because reusable containers outlive the JVM.
// DirtiesContext(AFTER_CLASS) is required too: a finished class's context stays cached and its
// listener keeps consuming the next arm's messages off the shared broker.
@Testcontainers
abstract class PipelineIntegrationTestBase {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.8");
  private static final DockerImageName RABBIT_IMAGE =
      DockerImageName.parse("rabbitmq:3.13-management-alpine");

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

  static final LocalStackContainer LOCALSTACK =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(Service.DYNAMODB).withReuse(true);
  static final RabbitMQContainer RABBIT = new RabbitMQContainer(RABBIT_IMAGE).withReuse(true);

  static final GenericContainer<?> REDIS =
      new GenericContainer<>(REDIS_IMAGE)
          .withExposedPorts(6379)
          // Redis accepts TCP before it will serve commands.
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
          .withReuse(true);

  static {
    LOCALSTACK.start();
    RABBIT.start();
    REDIS.start();
    applySchema();
    purgeQueues();
  }

  private static void applySchema() {
    try (DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(LOCALSTACK.getEndpoint())
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build()) {
      SchemaBootstrap.Result result = new SchemaBootstrap(client).apply();
      if (!result.drift().isEmpty()) {
        throw new IllegalStateException(
            "Schema drift: "
                + result.drift()
                + ", if this run reused containers (make it-reuse), the tables predate a schema"
                + " change; `make it-reuse-stop` and run again. Otherwise the schema tool"
                + " disagrees with what it just created.");
      }
    }
  }

  private static void purgeQueues() {
    CachingConnectionFactory connections =
        new CachingConnectionFactory(RABBIT.getHost(), RABBIT.getAmqpPort());
    connections.setUsername(RABBIT.getAdminUsername());
    connections.setPassword(RABBIT.getAdminPassword());
    try {
      RabbitAdmin admin = new RabbitAdmin(connections);
      for (String queue : List.of(Constants.MAIN_QUEUE, Constants.DLQ)) {
        if (admin.getQueueInfo(queue) != null) {
          admin.purgeQueue(queue, false);
        }
      }
    } finally {
      connections.destroy();
    }
  }

  @DynamicPropertySource
  static void configureSharedInfrastructure(DynamicPropertyRegistry registry) {
    registry.add("spring.rabbitmq.host", RABBIT::getHost);
    registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
    registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
    registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);

    registry.add("aws.region", LOCALSTACK::getRegion);
    registry.add("aws.dynamodb.endpoint", () -> LOCALSTACK.getEndpoint().toString());

    System.setProperty("aws.accessKeyId", LOCALSTACK.getAccessKey());
    System.setProperty("aws.secretAccessKey", LOCALSTACK.getSecretKey());

    registry.add("skier.writer.batch-size", () -> 5);
    registry.add("skier.writer.linger-ms", () -> 20);
    registry.add("skier.writer.threads", () -> 4);
    registry.add("skier.consumer.concurrency", () -> 2);
    registry.add("skier.consumer.prefetch", () -> 20);

    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
  }

  @Autowired protected RabbitTemplate rabbitTemplate;
  @Autowired protected RabbitAdmin rabbitAdmin;
  @Autowired protected DynamoDbClient dynamoDb;
  @Autowired protected SkierDataProcessor processor;
  @Autowired protected LiftRideWriter liftRideWriter;
  @Autowired protected UniqueSkierCounter uniqueSkierCounter;

  protected String skierId;

  protected abstract Class<? extends LiftRideWriter> expectedWriter();

  protected abstract Class<? extends UniqueSkierCounter> expectedCounter();

  @BeforeEach
  void setUp() {
    skierId = String.valueOf(Math.abs(UUID.randomUUID().hashCode() % 90_000) + 10_000);
  }

  protected void publish(String resortId, String dayId, int liftId, int time) {
    Map<String, Object> message = new HashMap<>();
    message.put("resortID", resortId);
    message.put("seasonID", "2025");
    message.put("dayID", dayId);
    message.put("skierID", skierId);
    message.put("liftID", liftId);
    message.put("time", time);

    String eventId = UUID.randomUUID().toString();
    MessagePostProcessor stamp =
        amqpMessage -> {
          amqpMessage.getMessageProperties().setHeader(Constants.HEADER_EVENT_ID, eventId);
          return amqpMessage;
        };

    rabbitTemplate.convertAndSend(
        Constants.LIFT_RIDE_EXCHANGE, Constants.LIFT_RIDE_ROUTING_KEY, message, stamp);
  }

  protected QueryRequest ridesBySkier() {
    return QueryRequest.builder()
        .tableName(Constants.LIFT_RIDES_TABLE)
        .keyConditionExpression(Constants.ATTR_SKIER_ID + " = :skier")
        .expressionAttributeValues(Map.of(":skier", AttributeValue.fromS(skierId)))
        .build();
  }

  protected int countRides() {
    return dynamoDb.query(ridesBySkier()).items().size();
  }

  protected int totalVertical() {
    return dynamoDb.query(ridesBySkier()).items().stream()
        .mapToInt(item -> Integer.parseInt(item.get(Constants.ATTR_VERTICAL).s()))
        .sum();
  }

  protected int uniqueSkierCount(String resortSeasonDay) {
    GetItemResponse response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(Constants.SKIER_COUNTS_TABLE)
                .key(
                    Map.of(Constants.ATTR_RESORT_SEASON_DAY, AttributeValue.fromS(resortSeasonDay)))
                .build());
    // v2 answers a missing item with an empty map, not null, so absence must be asked about.
    return response.hasItem()
        ? Integer.parseInt(response.item().get(Constants.ATTR_UNIQUE_SKIER_COUNT).n())
        : 0;
  }

  @Test
  @DisplayName("the write strategy under test is the one this suite configured")
  void writeStrategyUnderTestIsTheOneConfigured() {
    assertThat(liftRideWriter).isInstanceOf(expectedWriter());
  }

  @Test
  @DisplayName("the cardinality strategy under test is the one this suite configured")
  void cardinalityStrategyUnderTestIsTheOneConfigured() {
    assertThat(uniqueSkierCounter).isInstanceOf(expectedCounter());
  }

  @Test
  @DisplayName("a published event is consumed, written, and acknowledged")
  void endToEndSingleEvent() {
    publish("5", "1", 21, 217);

    await().atMost(Duration.ofSeconds(30)).until(() -> countRides() == 1);

    Map<String, AttributeValue> item = dynamoDb.query(ridesBySkier()).items().get(0);

    assertThat(item.get(Constants.ATTR_SORT_KEY).s()).isEqualTo("5#2025#1#217#21");
    assertThat(item.get(Constants.ATTR_VERTICAL).s()).isEqualTo("210");
    assertThat(item.get(Constants.ATTR_SKIER_SEASON).s()).isEqualTo(skierId + "#2025");
    assertThat(item.get(Constants.ATTR_RESORT_DAY).s()).isEqualTo("5#1");
    assertThat(item.get(Constants.ATTR_RESORT_SKIER).s()).isEqualTo("5#" + skierId);
    assertThat(item.get(Constants.ATTR_SEASON_DAY).s()).isEqualTo("2025#1");
    assertThat(item.get(Constants.ATTR_RESORT_SEASON_DAY).s()).isEqualTo("5#2025#1");

    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount() == 0);
    assertThat(processor.stagedCount()).isZero();
  }

  @Test
  @DisplayName("a burst is fully persisted")
  void endToEndBurst() {
    int events = 60;
    for (int i = 0; i < events; i++) {
      publish("5", "1", (i % 40) + 1, i + 1);
    }

    await().atMost(Duration.ofSeconds(60)).until(() -> countRides() == events);
    assertThat(totalVertical()).isPositive();
  }
}
