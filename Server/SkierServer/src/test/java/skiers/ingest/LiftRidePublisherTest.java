package skiers.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import skiers.Constants;
import skiers.config.SkierProperties;

class LiftRidePublisherTest {

  private static final Map<String, Object> BODY = Map.of("skierID", "42", "liftID", 21);

  private static class StubTemplate extends RabbitTemplate {
    private final AtomicReference<CorrelationData> captured = new AtomicReference<>();
    private final AtomicReference<MessagePostProcessor> envelope = new AtomicReference<>();
    private Runnable respond = () -> {};
    private RuntimeException sendFailure;

    @Override
    public void convertAndSend(
        String exchange,
        String routingKey,
        Object message,
        MessagePostProcessor postProcessor,
        CorrelationData correlationData) {
      if (sendFailure != null) {
        throw sendFailure;
      }
      envelope.set(postProcessor);
      captured.set(correlationData);
      respond.run();
    }

    void confirms() {
      respond = () -> captured.get().getFuture().complete(new CorrelationData.Confirm(true, null));
    }

    void nacks(String reason) {
      respond =
          () -> captured.get().getFuture().complete(new CorrelationData.Confirm(false, reason));
    }

    void returnsAsUnroutable() {
      respond =
          () -> {
            // RabbitMQ sends basic.return before basic.ack for an unroutable mandatory publish.
            captured
                .get()
                .setReturned(
                    new ReturnedMessage(
                        new Message(new byte[0]),
                        312,
                        "NO_ROUTE",
                        Constants.LIFT_RIDE_EXCHANGE,
                        "lift.ride.typo"));
            captured.get().getFuture().complete(new CorrelationData.Confirm(true, null));
          };
    }

    void neverAnswers() {
      respond = () -> {};
    }
  }

  private static ConnectionFactory confirmingConnectionFactory() {
    CachingConnectionFactory factory = new CachingConnectionFactory();
    factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
    factory.setPublisherReturns(true);
    return factory;
  }

  private static LiftRidePublisher publisher(RabbitTemplate template, long confirmTimeoutMs) {
    SkierProperties properties = new SkierProperties();
    properties.getIngest().setConfirmTimeoutMs(confirmTimeoutMs);
    return new LiftRidePublisher(template, confirmingConnectionFactory(), properties);
  }

  @Test
  @DisplayName("a broker ack with no return is the only outcome reported as confirmed")
  void confirmedOnAck() {
    StubTemplate template = new StubTemplate();
    template.confirms();

    assertThat(publisher(template, 1000).publish("evt-1", BODY, 1_700_000_000_000L))
        .isEqualTo(PublishOutcome.CONFIRMED);
  }

  @Test
  @DisplayName("a nacked publish is rejected, not accepted")
  void rejectedOnNack() {
    StubTemplate template = new StubTemplate();
    template.nacks("disk alarm");

    assertThat(publisher(template, 1000).publish("evt-2", BODY, 1_700_000_000_000L))
        .isEqualTo(PublishOutcome.REJECTED);
  }

  @Test
  @DisplayName("a publish that throws is rejected without waiting for a confirm")
  void rejectedWhenSendThrows() {
    StubTemplate template = new StubTemplate();
    template.sendFailure = new AmqpConnectException(new RuntimeException("broker down"));

    long startedAt = System.nanoTime();
    assertThat(publisher(template, 30_000).publish("evt-4", BODY, 1_700_000_000_000L))
        .isEqualTo(PublishOutcome.REJECTED);
    assertThat(System.nanoTime() - startedAt).isLessThan(5_000_000_000L);
  }

  @Test
  @DisplayName("a confirm that never arrives is unknown, not a failure")
  void unknownOnTimeout() {
    StubTemplate template = new StubTemplate();
    template.neverAnswers();

    assertThat(publisher(template, 50).publish("evt-5", BODY, 1_700_000_000_000L))
        .isEqualTo(PublishOutcome.UNKNOWN);
  }
}
