package skiers.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import skiers.config.SkierProperties;

class RedisFleetRegistryTest {

  private static SkierProperties properties() {
    SkierProperties properties = new SkierProperties();
    properties.getFleet().setHeartbeatIntervalMs(1000);
    properties.getFleet().setMemberTtlMs(5000);
    return properties;
  }

  /** A template whose zset operations answer {@code cardinality} and whose TIME answers 0. */
  private static StringRedisTemplate templateReporting(long cardinality) {
    StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    ZSetOperations<String, String> zset = mock(ZSetOperations.class);
    when(redis.opsForZSet()).thenReturn(zset);
    when(redis.execute(any(RedisCallback.class))).thenReturn(0L);
    when(zset.zCard(anyString())).thenReturn(cardinality);
    return redis;
  }

  @Test
  @DisplayName("a member TTL shorter than three heartbeats is refused at startup")
  void tooShortATtlIsRefused() {
    SkierProperties properties = properties();
    properties.getFleet().setHeartbeatIntervalMs(2000);
    properties.getFleet().setMemberTtlMs(4000);

    assertThatThrownBy(
            () ->
                new RedisFleetRegistry(
                    templateReporting(1L), properties, new SimpleMeterRegistry()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("skier.fleet.member-ttl-ms");
  }

  @Test
  @DisplayName("a successful refresh takes the size from ZCARD")
  void refreshReadsTheCardinality() {
    RedisFleetRegistry registry =
        new RedisFleetRegistry(templateReporting(3L), properties(), new SimpleMeterRegistry());

    assertThat(registry.size()).as("before the first refresh").isEqualTo(1);
    registry.refresh();
    assertThat(registry.size()).as("after one refresh").isEqualTo(3);
  }

  @Test
  @DisplayName("a failed refresh holds the last known size rather than falling back to one")
  void aFailedRefreshHoldsTheLastKnownSize() {
    StringRedisTemplate redis = templateReporting(2L);
    MeterRegistry meters = new SimpleMeterRegistry();
    AtomicLong clock = new AtomicLong();
    RedisFleetRegistry registry = new RedisFleetRegistry(redis, properties(), meters, clock::get);

    registry.refresh();
    assertThat(registry.size()).isEqualTo(2);

    when(redis.opsForZSet()).thenThrow(new IllegalStateException("connection refused"));
    clock.set(30_000);
    registry.refresh();

    assertThat(registry.size()).as("held through the outage").isEqualTo(2);
    assertThat(meters.get("skier.fleet.registry.failures").counter().count()).isEqualTo(1.0);
    assertThat(registry.stalenessSeconds())
        .as("and the scrape can tell a held value from a fresh one")
        .isEqualTo(30.0);
  }

  @Test
  @DisplayName("a null cardinality is a failure, not an empty fleet")
  void aNullCardinalityIsAFailure() {
    StringRedisTemplate redis = templateReporting(4L);
    MeterRegistry meters = new SimpleMeterRegistry();
    RedisFleetRegistry registry = new RedisFleetRegistry(redis, properties(), meters);
    registry.refresh();
    assertThat(registry.size()).isEqualTo(4);

    when(redis.opsForZSet().zCard(anyString())).thenReturn(null);
    registry.refresh();

    assertThat(registry.size()).isEqualTo(4);
    assertThat(meters.get("skier.fleet.registry.failures").counter().count()).isEqualTo(1.0);
  }
}
