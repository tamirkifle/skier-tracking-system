package skiers.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/** Read-path cache. TTLs are per cache, sized to how fast each value changes. */
@Configuration
@EnableCaching
public class CacheConfig {

  private static final Logger logger = LoggerFactory.getLogger(CacheConfig.class);

  static final Duration SKIER_DAY_VERTICAL_TTL = Duration.ofSeconds(10);
  static final Duration RESORT_SKIER_COUNT_TTL = Duration.ofSeconds(30);
  static final Duration SKIER_RESORT_TOTALS_TTL = Duration.ofMinutes(5);

  @Bean
  public RedisCacheManager cacheManager(
      RedisConnectionFactory connectionFactory, SkierProperties properties) {

    RedisCacheConfiguration defaults =
        RedisCacheConfiguration.defaultCacheConfig()
            .entryTtl(properties.getCache().getTtl())
            .disableCachingNullValues()
            .serializeKeysWith(
                RedisSerializationContext.SerializationPair.fromSerializer(
                    new StringRedisSerializer()))
            .serializeValuesWith(
                RedisSerializationContext.SerializationPair.fromSerializer(
                    new GenericJackson2JsonRedisSerializer()));

    return RedisCacheManager.builder(connectionFactory)
        .cacheDefaults(defaults)
        .withInitialCacheConfigurations(
            Map.of(
                "skierDayVertical", defaults.entryTtl(SKIER_DAY_VERTICAL_TTL),
                "resortSkierCount", defaults.entryTtl(RESORT_SKIER_COUNT_TTL),
                "skierResortTotals", defaults.entryTtl(SKIER_RESORT_TOTALS_TTL)))
        .build();
  }

  /** Spring's default handler rethrows, so a Redis outage would fail every read. */
  @Bean
  public CacheErrorHandler cacheErrorHandler(MeterRegistry registry) {
    Counter errors =
        Counter.builder("skier.cache.errors")
            .description("Cache operations that failed and were bypassed")
            .register(registry);

    return new CacheErrorHandler() {
      @Override
      public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
        report("get", cache, key, exception);
      }

      @Override
      public void handleCachePutError(
          RuntimeException exception, Cache cache, Object key, Object value) {
        report("put", cache, key, exception);
      }

      @Override
      public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {
        report("evict", cache, key, exception);
      }

      @Override
      public void handleCacheClearError(RuntimeException exception, Cache cache) {
        report("clear", cache, null, exception);
      }

      private void report(String operation, Cache cache, Object key, RuntimeException exception) {
        errors.increment();
        logger.warn(
            "Cache {} bypassed on '{}' (key={}): {}",
            operation,
            cache.getName(),
            key,
            exception.getMessage());
      }
    };
  }
}
