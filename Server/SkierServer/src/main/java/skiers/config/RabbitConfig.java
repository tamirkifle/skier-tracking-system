package skiers.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import skiers.Constants;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitConfig {

  @Value("${spring.rabbitmq.host}")
  private String host;

  @Value("${spring.rabbitmq.port:5672}")
  private int port;

  @Value("${spring.rabbitmq.username:admin}")
  private String username;

  @Value("${spring.rabbitmq.password:admin-password}")
  private String password;

  @Bean
  public Queue mainQueue() {
    Map<String, Object> args = new HashMap<>();
    args.put("x-dead-letter-exchange", Constants.DLX);
    return new Queue(Constants.MAIN_QUEUE, true, false, false, args);
  }

  @Bean
  public TopicExchange mainExchange() {
    return new TopicExchange(Constants.LIFT_RIDE_EXCHANGE, true, false);
  }

  @Bean
  public Binding binding(Queue mainQueue, TopicExchange mainExchange) {
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

  @Bean
  public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    return new RabbitAdmin(connectionFactory);
  }

  @Bean
  public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
    RabbitTemplate template = new RabbitTemplate(connectionFactory);
    template.setMessageConverter(new Jackson2JsonMessageConverter());
    return template;
  }

  @Bean
  public ConnectionFactory connectionFactory() {
    CachingConnectionFactory factory = new CachingConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);
    factory.setRequestedHeartBeat(60);
    factory.setChannelCacheSize(300);
    return factory;
  }
}