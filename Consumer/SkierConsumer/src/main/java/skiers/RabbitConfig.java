package skiers;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {
  public static final String MAIN_QUEUE = "liftRideQueue";
  public static final String DLX = "deadLetterExchange";
  public static final String DLQ = "deadLetterQueue";

  @Value("${spring.rabbitmq.host}") private String host;
  @Value("${spring.rabbitmq.port}") private int port;
  @Value("${spring.rabbitmq.username}") private String username;
  @Value("${spring.rabbitmq.password}") private String password;


  // Update all string constants to use Constants class
  @Bean
  public Queue mainQueue() {
    return QueueBuilder.durable(Constants.MAIN_QUEUE)
        .withArgument("x-dead-letter-exchange", Constants.DLX)
        .build();
  }

  @Bean
  public TopicExchange mainExchange() {
    return new TopicExchange(Constants.LIFT_RIDE_EXCHANGE);
  }

  @Bean
  public Binding mainBinding(Queue mainQueue, TopicExchange mainExchange) {
    return BindingBuilder.bind(mainQueue)
        .to(mainExchange)
        .with(Constants.LIFT_RIDE_ROUTING_KEY);
  }

  @Bean
  public TopicExchange deadLetterExchange() {
    return new TopicExchange(Constants.DLX);
  }

  @Bean
  public Queue deadLetterQueue() {
    return QueueBuilder.durable(Constants.DLQ).build();
  }

  @Bean
  public Binding deadLetterBinding() {
    return BindingBuilder.bind(deadLetterQueue())
        .to(deadLetterExchange())
        .with(Constants.DEAD_LETTER_ROUTING_KEY);
  }

  // Update the listener container factory
  @Bean
  public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
      ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setConcurrentConsumers(Constants.CONCURRENT_CONSUMERS);
    factory.setMaxConcurrentConsumers(Constants.MAX_CONCURRENT_CONSUMERS);
    factory.setPrefetchCount(Constants.PREFETCH_COUNT);
    factory.setMessageConverter(new Jackson2JsonMessageConverter());
    return factory;
  }

  @Bean
  public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    return new RabbitAdmin(connectionFactory);
  }
}