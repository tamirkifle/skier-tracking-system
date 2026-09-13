package skiers.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
import skiers.infra.SchemaBootstrap;
import skiers.service.RateLimiter;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Tag("integration")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
class IngestIntegrationTest {

  private static final DockerImageName RABBIT_IMAGE =
      DockerImageName.parse("rabbitmq:3.13-management-alpine");
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.8");
  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

  static final RabbitMQContainer RABBIT = new RabbitMQContainer(RABBIT_IMAGE).withReuse(true);

  static final LocalStackContainer LOCALSTACK =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(Service.DYNAMODB).withReuse(true);

  static final GenericContainer<?> REDIS =
      new GenericContainer<>(REDIS_IMAGE)
          .withExposedPorts(6379)
          // Redis accepts TCP a moment before it will serve commands.
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
          .withReuse(true);

  static {
    RABBIT.start();
    LOCALSTACK.start();
    REDIS.start();
    applySchema();
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
      new SchemaBootstrap(client).apply();
    }
  }

  @DynamicPropertySource
  static void configure(DynamicPropertyRegistry registry) {
    registry.add("spring.rabbitmq.host", RABBIT::getHost);
    registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
    registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
    registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);

    registry.add("aws.region", LOCALSTACK::getRegion);
    registry.add("aws.dynamodb.endpoint", () -> LOCALSTACK.getEndpoint().toString());

    // aws.secretKey is the v1 name; v2 reads aws.secretAccessKey, and the chain needs one.
    System.setProperty("aws.accessKeyId", LOCALSTACK.getAccessKey());
    System.setProperty("aws.secretAccessKey", LOCALSTACK.getSecretKey());

    registry.add("skier.queue-monitor.enabled", () -> false);

    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));

    registry.add("spring.cache.type", () -> "simple");

    registry.add("management.endpoint.health.show-details", () -> "always");

    registry.add("skier.admission.max-wait-ms", () -> 0);
  }

  @Autowired private TestRestTemplate rest;
  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private RabbitAdmin rabbitAdmin;
  @Autowired private RateLimiter rateLimiter;
  @Autowired private ObjectMapper objectMapper;

  private static final String PATH = "/skiers/5/seasons/2025/days/1/skier/{skierId}";

  @BeforeEach
  void drainQueue() {
    rabbitAdmin.purgeQueue(Constants.MAIN_QUEUE, false);
    rateLimiter.adjustRateUp(Constants.MAX_RATE);
  }

  private ResponseEntity<String> post(int skierId, String body) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    return rest.postForEntity(PATH, new HttpEntity<>(body, headers), String.class, skierId);
  }

  @Test
  @DisplayName("a valid event is published to a real broker and is routable")
  void publishesRoutableMessage() throws Exception {
    ResponseEntity<String> response = post(42, "{\"liftID\":21,\"time\":217}");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(response.getHeaders().getFirst(Constants.HEADER_EVENT_ID)).isNotBlank();

    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount() == 1);

    Message received = rabbitTemplate.receive(Constants.MAIN_QUEUE, 5000);
    assertThat(received).isNotNull();

    @SuppressWarnings("unchecked")
    Map<String, Object> payload = objectMapper.readValue(received.getBody(), Map.class);
    assertThat(payload)
        .containsEntry("resortID", "5")
        .containsEntry("seasonID", "2025")
        .containsEntry("dayID", "1")
        .containsEntry("skierID", "42")
        .containsEntry("liftID", 21)
        .containsEntry("time", 217);

    // MessageProperties.getHeader is declared <T> T, so assertThat needs a typed local.
    String eventIdHeader = received.getMessageProperties().getHeader(Constants.HEADER_EVENT_ID);
    assertThat(eventIdHeader).isEqualTo(response.getHeaders().getFirst(Constants.HEADER_EVENT_ID));
    assertThat(received.getMessageProperties().getContentType())
        .isEqualTo(MediaType.APPLICATION_JSON_VALUE);
  }

  @Test
  @DisplayName("an unroutable publish is 503, not the 201 the broker's own ack would suggest")
  void unroutablePublishIsNotAccepted() {
    Binding binding =
        BindingBuilder.bind(new Queue(Constants.MAIN_QUEUE))
            .to(new TopicExchange(Constants.LIFT_RIDE_EXCHANGE))
            .with(Constants.LIFT_RIDE_ROUTING_KEY);
    rabbitAdmin.removeBinding(binding);

    try {
      // The exchange still exists, so RabbitMQ returns the message and then acks it.
      ResponseEntity<String> response = post(44, "{\"liftID\":10,\"time\":100}");

      assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
      assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount()).isZero();
    } finally {
      rabbitAdmin.declareBinding(binding);
    }
  }

  @Test
  @DisplayName("a client-supplied event id reaches the broker envelope unchanged")
  void clientEventIdSurvivesToTheEnvelope() {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set(Constants.HEADER_EVENT_ID, "retry-of-unknown-outcome");

    ResponseEntity<String> response =
        rest.postForEntity(
            PATH, new HttpEntity<>("{\"liftID\":10,\"time\":100}", headers), String.class, 45);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount() == 1);

    Message received = rabbitTemplate.receive(Constants.MAIN_QUEUE, 5000);
    String eventIdHeader = received.getMessageProperties().getHeader(Constants.HEADER_EVENT_ID);
    assertThat(eventIdHeader).isEqualTo("retry-of-unknown-outcome");
    assertThat(received.getMessageProperties().getMessageId())
        .isEqualTo("retry-of-unknown-outcome");
  }

  @Test
  @DisplayName("messages are published as persistent, so a broker restart does not lose them")
  void publishesPersistentMessages() {
    post(43, "{\"liftID\":10,\"time\":100}");

    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount() == 1);
    Message received = rabbitTemplate.receive(Constants.MAIN_QUEUE, 5000);

    assertThat(received.getMessageProperties().getReceivedDeliveryMode())
        .isEqualTo(org.springframework.amqp.core.MessageDeliveryMode.PERSISTENT);
  }

  @Test
  @DisplayName("both services' topology declarations agree, so either may start first")
  void declaresCompatibleTopology() {
    assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE)).isNotNull();
    assertThat(rabbitAdmin.getQueueInfo(Constants.DLQ)).isNotNull();
  }

  @Test
  @DisplayName("under shedding, exactly the accepted events reach the queue")
  void shedsWithoutPublishing() {
    rateLimiter.adjustRateDown(1);
    while (rateLimiter.tryAcquire()) {
      // drain
    }

    int offered = 300;
    int accepted = 0;
    int shed = 0;
    for (int i = 0; i < offered; i++) {
      ResponseEntity<String> response =
          post(2000 + i, "{\"liftID\":" + ((i % 40) + 1) + ",\"time\":" + ((i % 360) + 1) + "}");
      if (response.getStatusCode() == HttpStatus.CREATED) {
        accepted++;
      } else if (response.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
        shed++;
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
      }
    }

    assertThat(shed)
        .as("offering %d requests at the floor rate must shed some", offered)
        .isPositive();
    assertThat(accepted + shed).isEqualTo(offered);

    final int expectedOnQueue = accepted;

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () ->
                assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount())
                    .isEqualTo(expectedOnQueue));
  }

  @Test
  @DisplayName("an invalid body is rejected without spending a permit or publishing")
  void rejectsInvalidWithoutPublishing() {
    long grantedBefore = rateLimiter.grantedCount();

    assertThat(post(45, "{\"liftID\":9999,\"time\":217}").getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(post(45, "{}").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

    assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount()).isZero();
    assertThat(rateLimiter.grantedCount()).isEqualTo(grantedBefore);
  }

  @Test
  @DisplayName("a burst of valid events all reach the queue")
  void publishesEveryEventInABurst() {
    int events = 200;
    for (int i = 0; i < events; i++) {
      assertThat(
              post(1000 + i, "{\"liftID\":" + ((i % 40) + 1) + ",\"time\":" + ((i % 360) + 1) + "}")
                  .getStatusCode())
          .isEqualTo(HttpStatus.CREATED);
    }

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(rabbitAdmin.getQueueInfo(Constants.MAIN_QUEUE).getMessageCount())
                    .isEqualTo(events));
  }

  @Test
  @DisplayName("actuator reports the pipeline's own state")
  void exposesPipelineHealth() throws Exception {
    ResponseEntity<String> health = rest.getForEntity("/actuator/health", String.class);
    assertThat(health.getStatusCode())
        .as("health body: %s", health.getBody())
        .isEqualTo(HttpStatus.OK);

    @SuppressWarnings("unchecked")
    Map<String, Object> body = objectMapper.readValue(health.getBody(), Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> components =
        (Map<String, Map<String, Object>>) body.get("components");
    assertThat(components)
        .as("every dependency the service cannot serve a request without must be reported on")
        .containsKeys("rabbit", "dynamoDb", "redis", "pipeline");
    assertThat(components.get("rabbit")).containsEntry("status", "UP");
    assertThat(components.get("dynamoDb")).containsEntry("status", "UP");
    assertThat(components.get("redis")).containsEntry("status", "UP");

    ResponseEntity<String> readiness =
        rest.getForEntity("/actuator/health/readiness", String.class);
    assertThat(readiness.getStatusCode())
        .as("readiness body: %s", readiness.getBody())
        .isEqualTo(HttpStatus.OK);
    @SuppressWarnings("unchecked")
    Map<String, Object> readinessBody = objectMapper.readValue(readiness.getBody(), Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> readinessComponents =
        (Map<String, Map<String, Object>>) readinessBody.get("components");
    assertThat(readinessComponents)
        .as("readiness must depend on this instance having counted its own fleet")
        .containsKeys("readinessState", "fleetRegistry");
    assertThat(readinessComponents.get("fleetRegistry"))
        .containsEntry("status", "UP")
        .extracting(component -> component.get("details"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("sized", true);

    ResponseEntity<String> metrics = rest.getForEntity("/actuator/prometheus", String.class);
    assertThat(metrics.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(metrics.getBody())
        .contains("skier_admission_rate")
        .contains("skier_ingest_accepted_total")
        .contains("skier_ingest_shed_total");
  }
}
