package skiers.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import skiers.Constants;

@Configuration
public class AwsConfig {

  @Value("${aws.accessKeyId:}")
  private String accessKey;

  @Value("${aws.secretKey:}")
  private String secretKey;

  @Value("${aws.sessionToken:}")
  private String sessionToken;

  @Value("${aws.region:us-west-2}")
  private String region;

  @Bean
  public AmazonDynamoDB amazonDynamoDB() {
    if (StringUtils.hasText(sessionToken)) {
      // Use session credentials if token exists
      BasicSessionCredentials credentials = new BasicSessionCredentials(
          accessKey,
          secretKey,
          sessionToken
      );
      return AmazonDynamoDBClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withRegion(region)
          .build();
    } else if (StringUtils.hasText(accessKey)){
      // Use basic credentials if no token
      return AmazonDynamoDBClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(accessKey, secretKey)))
          .withRegion(region)
          .build();
    } else {
      // Default to instance profile credentials
      return AmazonDynamoDBClientBuilder.standard()
          .withRegion(region)
          .build();
    }
  }

  // AwsConfig.java - Configure high-throughput client
  @Bean
  public AmazonDynamoDB dynamoDBClient() {
    return AmazonDynamoDBClientBuilder.standard()
        .withRegion(Regions.US_WEST_2)
        .withClientConfiguration(new ClientConfiguration()
            .withMaxConnections(Constants.MAX_DB_CONNECTION)
            .withConnectionTimeout(Constants.DB_CONNECTION_TIMEOUT)
            .withRequestTimeout(Constants.DB_REQUEST_TIMEOUT))
        .build();
  }
}