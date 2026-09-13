package skiers.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class RabbitTemplateSettingsTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              org.springframework.boot.autoconfigure.AutoConfigurations.of(
                  RabbitAutoConfiguration.class))
          .withUserConfiguration(RabbitConfig.class);

  @Test
  @DisplayName("the declared template is mandatory, so an unroutable publish is returned")
  void templateIsMandatory() {
    runner
        .withPropertyValues(
            "spring.rabbitmq.template.mandatory=true",
            "spring.rabbitmq.publisher-confirm-type=correlated",
            "spring.rabbitmq.publisher-returns=true")
        .run(
            context -> {
              RabbitTemplate template = context.getBean(RabbitTemplate.class);
              assertThat(template.isMandatoryFor(new Message(new byte[0]))).isTrue();
            });
  }

  @Test
  @DisplayName("the property is what decides it, not a literal in the configuration class")
  void mandatoryComesFromTheProperty() {
    runner
        .withPropertyValues("spring.rabbitmq.template.mandatory=false")
        .run(
            context -> {
              RabbitTemplate template = context.getBean(RabbitTemplate.class);
              assertThat(template.isMandatoryFor(new Message(new byte[0]))).isFalse();
            });
  }

  @Test
  @DisplayName("publisher confirms and returns are enabled on the connection factory")
  void connectionFactoryConfirmsAndReturns() {
    runner
        .withPropertyValues(
            "spring.rabbitmq.publisher-confirm-type=correlated",
            "spring.rabbitmq.publisher-returns=true")
        .run(
            context -> {
              org.springframework.amqp.rabbit.connection.ConnectionFactory factory =
                  context.getBean(
                      org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
              assertThat(factory.isPublisherConfirms()).isTrue();
              assertThat(factory.isPublisherReturns()).isTrue();
            });
  }
}
