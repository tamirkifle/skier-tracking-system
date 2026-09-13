package skiers.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import skiers.Constants;

@Configuration
public class RabbitConfig {

  private static final Logger logger = LoggerFactory.getLogger(RabbitConfig.class);

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
}
