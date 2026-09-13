package skiers.ingest;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class AmqpDeliveryAckTest {

  private Channel channel;

  @BeforeEach
  void setUp() {
    channel = mock(Channel.class);
  }

  private static Message delivery() {
    MessageProperties properties = new MessageProperties();
    properties.setMessageId("evt-1");
    return new Message("{}".getBytes(StandardCharsets.UTF_8), properties);
  }

  private AmqpDeliveryAck ack(long tag) {
    return new AmqpDeliveryAck(channel, tag, delivery(), RetryRouter.NONE);
  }

  private AmqpDeliveryAck ack(long tag, RetryRouter router) {
    return new AmqpDeliveryAck(channel, tag, delivery(), router);
  }

  @Test
  @DisplayName("ack settles the delivery once")
  void acksOnce() throws Exception {
    ack(7L).ack();
    verify(channel).basicAck(7L, false);
  }

  @Test
  @DisplayName("reject with requeue returns the message to the queue")
  void requeues() throws Exception {
    ack(7L).reject(true);
    verify(channel).basicNack(7L, false, true);
  }

  @Test
  @DisplayName("reject without requeue routes to the dead-letter exchange")
  void deadLetters() throws Exception {
    ack(7L).reject(false);
    verify(channel).basicNack(7L, false, false);
  }

  @Test
  @DisplayName("a second settlement is ignored rather than sent")
  void settlesAtMostOnce() throws Exception {
    AmqpDeliveryAck ack = ack(7L);

    ack.ack();
    ack.ack();
    ack.reject(true);
    ack.reject(false);

    verify(channel, times(1)).basicAck(anyLong(), anyBoolean());
    verify(channel, times(0)).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  @Test
  @DisplayName("concurrent settlement attempts produce exactly one frame")
  void settlesOnceUnderContention() throws Exception {
    AmqpDeliveryAck ack = ack(7L);

    ExecutorService pool = Executors.newFixedThreadPool(8);
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      tasks.add(
          () -> {
            ack.ack();
            return null;
          });
    }
    for (Future<Void> future : pool.invokeAll(tasks)) {
      future.get();
    }
    pool.shutdownNow();

    verify(channel, times(1)).basicAck(anyLong(), anyBoolean());
  }

  @Test
  @DisplayName("a failed ack does not propagate")
  void swallowsAckFailure() throws Exception {
    doThrow(new IOException("channel closed")).when(channel).basicAck(anyLong(), anyBoolean());

    assertThatCode(() -> ack(7L).ack()).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("a failed nack does not propagate")
  void swallowsNackFailure() throws Exception {
    doThrow(new IOException("channel closed"))
        .when(channel)
        .basicNack(anyLong(), anyBoolean(), anyBoolean());

    assertThatCode(() -> ack(7L).reject(true)).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("the no-op settlement is safe to call")
  void noneIsInert() {
    assertThatCode(
            () -> {
              DeliveryAck.NONE.ack();
              DeliveryAck.NONE.reject(true);
              DeliveryAck.NONE.reject(false);
              DeliveryAck.NONE.retryLater(1);
            })
        .doesNotThrowAnyException();
  }
}
