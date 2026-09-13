package skiers;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import skiers.config.ConsumerProperties;

@Configuration
public class RabbitConfig {

  /** Deliberately unbounded; server-side admission control, not x-max-length, bounds depth. */
  @Bean
  public Queue mainQueue() {
    return QueueBuilder.durable(Constants.MAIN_QUEUE)
        .withArgument("x-dead-letter-exchange", Constants.DLX)
        .withArgument("x-dead-letter-routing-key", Constants.DEAD_LETTER_ROUTING_KEY)
        .build();
  }

  @Bean
  public TopicExchange mainExchange() {
    return new TopicExchange(Constants.LIFT_RIDE_EXCHANGE, true, false);
  }

  @Bean
  public Binding mainBinding(Queue mainQueue, TopicExchange mainExchange) {
    return BindingBuilder.bind(mainQueue).to(mainExchange).with(Constants.LIFT_RIDE_ROUTING_KEY);
  }

  @Bean
  public TopicExchange deadLetterExchange() {
    return new TopicExchange(Constants.DLX, true, false);
  }

  @Bean
  public Queue deadLetterQueue() {
    return QueueBuilder.durable(Constants.DLQ).build();
  }

  /** The source queue must set a dead-letter routing key, or a rejection misses this binding. */
  @Bean
  public Binding deadLetterBinding(Queue deadLetterQueue, TopicExchange deadLetterExchange) {
    return BindingBuilder.bind(deadLetterQueue)
        .to(deadLetterExchange)
        .with(Constants.DEAD_LETTER_ROUTING_KEY);
  }

  @Bean
  public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
      ConnectionFactory connectionFactory,
      ConsumerProperties properties,
      MessageConverter messageConverter) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setConcurrentConsumers(properties.getConsumer().getConcurrency());
    factory.setMaxConcurrentConsumers(properties.getConsumer().getMaxConcurrency());
    factory.setPrefetchCount(properties.getConsumer().getPrefetch());
    factory.setMessageConverter(messageConverter);
    factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    return factory;
  }

  /** A bean so the auto-configured template shares it; the default is Java serialization. */
  @Bean
  public MessageConverter jsonMessageConverter() {
    return new Jackson2JsonMessageConverter();
  }

  @Bean
  public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    return new RabbitAdmin(connectionFactory);
  }
}
