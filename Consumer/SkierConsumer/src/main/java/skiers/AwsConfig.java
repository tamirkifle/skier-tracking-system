package skiers;

import java.net.URI;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration
public class AwsConfig {

  private static final Logger logger = LoggerFactory.getLogger(AwsConfig.class);

  @Value("${aws.region:us-west-2}")
  private String region;

  @Value("${aws.dynamodb.endpoint:}")
  private String dynamoDbEndpoint;

  /** Pool size is set rather than defaulted; it caps the single-put path's throughput. */
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
}
