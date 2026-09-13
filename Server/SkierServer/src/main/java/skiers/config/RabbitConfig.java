package skiers.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import skiers.Constants;

@Configuration
public class RabbitConfig {

  private static final Logger logger = LoggerFactory.getLogger(RabbitConfig.class);

  @Bean
  public Queue mainQueue() {
    return QueueBuilder.durable(Constants.MAIN_QUEUE)
        .withArgument("x-dead-letter-exchange", Constants.DLX)
        // Without this, a dead-lettered message keeps lift.ride and misses the DLQ binding.
        .withArgument("x-dead-letter-routing-key", Constants.DEAD_LETTER_ROUTING_KEY)
        .build();
  }

  @Bean
  public TopicExchange mainExchange() {
    return new TopicExchange(Constants.LIFT_RIDE_EXCHANGE, true, false);
  }

  @Bean
  public Binding binding(Queue mainQueue, TopicExchange mainExchange) {
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

  @Bean
  public Binding deadLetterBinding(Queue deadLetterQueue, TopicExchange deadLetterExchange) {
    return BindingBuilder.bind(deadLetterQueue)
        .to(deadLetterExchange)
        .with(Constants.DEAD_LETTER_ROUTING_KEY);
  }

  @Bean
  public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    return new RabbitAdmin(connectionFactory);
  }
}
