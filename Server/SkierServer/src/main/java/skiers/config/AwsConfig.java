package skiers.config;

import java.net.URI;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import skiers.Constants;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** DynamoDB client wiring; credentials come only from the SDK default provider chain. */
@Configuration
public class AwsConfig {

  private static final Logger logger = LoggerFactory.getLogger(AwsConfig.class);

  @Value("${aws.region:us-west-2}")
  private String region;

  @Value("${aws.dynamodb.endpoint:}")
  private String dynamoDbEndpoint;

  /** Pool size is stated, not defaulted: every read is a blocking call on a request thread. */
  @Bean(destroyMethod = "close")
  public DynamoDbClient dynamoDbClient() {
    var httpClient =
        ApacheHttpClient.builder()
            .maxConnections(Constants.MAX_DB_CONNECTION)
            .connectionTimeout(Duration.ofMillis(Constants.DB_CONNECTION_TIMEOUT));

    var builder =
        DynamoDbClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.of(region))
            .httpClientBuilder(httpClient)
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMillis(Constants.DB_REQUEST_TIMEOUT))
                    .build());

    if (StringUtils.hasText(dynamoDbEndpoint)) {
      logger.info("DynamoDB (v2 sync) pointed at local endpoint {}", dynamoDbEndpoint);
      builder.endpointOverride(URI.create(dynamoDbEndpoint));
    }

    return builder.build();
  }

  @Bean(destroyMethod = "close")
  public DynamoDbAsyncClient dynamoDbAsyncClient() {
    var builder =
        DynamoDbAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.of(region))
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMillis(Constants.DB_REQUEST_TIMEOUT))
                    .build());

    if (StringUtils.hasText(dynamoDbEndpoint)) {
      logger.info("DynamoDB (v2 async) pointed at local endpoint {}", dynamoDbEndpoint);
      builder.endpointOverride(URI.create(dynamoDbEndpoint));
    }

    return builder.build();
  }
}
