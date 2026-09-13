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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
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

  @Bean
  public MessageConverter jsonMessageConverter() {
    return new Jackson2JsonMessageConverter();
  }

  /** Built by RabbitTemplateConfigurer, so spring.rabbitmq.template.mandatory applies. */
  @Bean
  public RabbitTemplate rabbitTemplate(
      RabbitTemplateConfigurer configurer,
      ConnectionFactory connectionFactory,
      MessageConverter messageConverter) {
    RabbitTemplate template = new RabbitTemplate();
    configurer.configure(template, connectionFactory);
    template.setMessageConverter(messageConverter);

    template.setConfirmCallback(
        (correlation, acked, cause) -> {
          if (!acked) {
            logger.error(
                "Broker nacked publish (correlation={}): {}",
                correlation == null ? "none" : correlation.getId(),
                cause);
          }
        });

    template.setReturnsCallback(
        returned ->
            logger.error(
                "Message returned as unroutable: exchange={} routingKey={} replyText={}",
                returned.getExchange(),
                returned.getRoutingKey(),
                returned.getReplyText()));

    return template;
  }
}
