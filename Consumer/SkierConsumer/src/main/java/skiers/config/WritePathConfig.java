package skiers.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import skiers.cardinality.DynamoDbUniqueSkierCounter;
import skiers.cardinality.RedisHyperLogLogCounter;
import skiers.cardinality.UniqueSkierCounter;
import skiers.metrics.ConsumerMetrics;
import skiers.persistence.BatchingLiftRideWriter;
import skiers.persistence.LiftRideWriter;
import skiers.persistence.SinglePutLiftRideWriter;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration
public class WritePathConfig {

  private static final Logger logger = LoggerFactory.getLogger(WritePathConfig.class);

  @Bean
  public LiftRideWriter liftRideWriter(
      ConsumerProperties properties,
      DynamoDbClient dynamoDbClient,
      ObjectProvider<DynamoDbAsyncClient> asyncClient) {

    ConsumerProperties.Writer.Mode mode = properties.getWriter().getMode();
    if (mode == ConsumerProperties.Writer.Mode.SINGLE) {
      logger.info("Write strategy: single PutItem per event");
      return new SinglePutLiftRideWriter(dynamoDbClient);
    }

    DynamoDbAsyncClient client = asyncClient.getIfAvailable();
    if (client == null) {
      logger.warn(
          "Batch mode requested but no async DynamoDB client is available; using single-put");
      return new SinglePutLiftRideWriter(dynamoDbClient);
    }

    logger.info(
        "Write strategy: BatchWriteItem, up to {} items per request",
        properties.getWriter().getBatchSize());
    return new BatchingLiftRideWriter(client, properties.getWriter().getBatchSize());
  }

  @Bean
  public UniqueSkierCounter uniqueSkierCounter(
      ConsumerProperties properties,
      DynamoDbClient dynamoDbClient,
      ConsumerMetrics metrics,
      ObjectProvider<StringRedisTemplate> redis) {

    if (properties.getCardinality().getStrategy()
        == ConsumerProperties.Cardinality.Strategy.REDIS_HLL) {
      StringRedisTemplate template = redis.getIfAvailable();
      if (template != null) {
        logger.info("Cardinality strategy: Redis HyperLogLog (approximate, ~0.81% error)");
        // The counter mirrors PFCOUNT into SkierCounts, which the read API serves.
        return new RedisHyperLogLogCounter(template, dynamoDbClient, metrics);
      }
      logger.warn("redis-hll requested but no Redis template is available; using DynamoDB counter");
    }

    logger.info("Cardinality strategy: DynamoDB conditional sentinel (exact)");
    return new DynamoDbUniqueSkierCounter(dynamoDbClient, metrics);
  }
}
