package skiers.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import skiers.config.SkierProperties;

/**
 * Counts the server instances sharing the queue, using a Redis sorted set of heartbeats. Each
 * instance holds one member keyed by a per-process id; a refresh writes its own member, drops every
 * member older than {@code member-ttl-ms}, and reads the cardinality.
 */
public class RedisFleetRegistry implements FleetRegistry {

  private static final Logger logger = LoggerFactory.getLogger(RedisFleetRegistry.class);

  private final StringRedisTemplate redis;
  private final SkierProperties.Fleet config;

  private final String instanceId = UUID.randomUUID().toString();

  private final AtomicInteger observed = new AtomicInteger(1);

  /** False until the first successful refresh. Never returns to false; an outage holds instead. */
  private volatile boolean sized;

  private final AtomicLong lastRefreshMillis;
  private final AtomicInteger consecutiveFailures = new AtomicInteger();
  private final Counter refreshFailures;
  private final Counter observedShrinks;

  private int pendingShrinkSize = -1;

  private int pendingShrinkStreak;

  private final java.util.function.LongSupplier epochMillis;

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "fleet-heartbeat");
            thread.setDaemon(true);
            return thread;
          });

  public RedisFleetRegistry(
      StringRedisTemplate redis, SkierProperties properties, MeterRegistry registry) {
    this(redis, properties, registry, System::currentTimeMillis);
  }

  RedisFleetRegistry(
      StringRedisTemplate redis,
      SkierProperties properties,
      MeterRegistry registry,
      java.util.function.LongSupplier epochMillis) {
    this.redis = redis;
    this.config = properties.getFleet();
    this.epochMillis = epochMillis;
    this.lastRefreshMillis = new AtomicLong(epochMillis.getAsLong());

    // A TTL at or below the heartbeat interval evicts live instances on every beat, so the observed
    // size collapses to 1 and each one takes the whole floor. Three beats tolerates one lost trip.
    if (config.getMemberTtlMs() < 3 * config.getHeartbeatIntervalMs()) {
      throw new IllegalArgumentException(
          "skier.fleet.member-ttl-ms must be at least 3x skier.fleet.heartbeat-interval-ms"
              + " (a shorter TTL evicts live instances and silently restores per-instance"
              + " admission) but was "
              + config.getMemberTtlMs()
              + "ms against a heartbeat of "
              + config.getHeartbeatIntervalMs()
              + "ms");
    }

    Gauge.builder("skier.fleet.registry.staleness", this, RedisFleetRegistry::stalenessSeconds)
        .description("Seconds since the fleet size was last refreshed from Redis")
        .baseUnit("seconds")
        .register(registry);
    this.refreshFailures =
        Counter.builder("skier.fleet.registry.failures")
            .description("Fleet-size refreshes that could not reach Redis")
            .register(registry);
    this.observedShrinks =
        Counter.builder("skier.fleet.registry.shrink")
            .description(
                "Successful refreshes that observed a smaller fleet than the previous observation")
            .register(registry);

    logger.info(
        "Fleet coordination armed: instance {} heartbeating every {}ms into {} with a {}ms member"
            + " TTL",
        instanceId,
        config.getHeartbeatIntervalMs(),
        config.getKey(),
        config.getMemberTtlMs());
  }

  @Override
  public int size() {
    return observed.get();
  }

  @Override
  public boolean isSized() {
    return sized;
  }

  public double stalenessSeconds() {
    return (epochMillis.getAsLong() - lastRefreshMillis.get()) / 1000.0;
  }

  /**
   * Deliberately not {@code @Scheduled}: Boot's default scheduling pool holds one thread, which
   * {@link QueueMonitor} already occupies with a synchronous broker round trip five times a second.
   * A heartbeat sharing it misses its own member TTL exactly when the system is loaded.
   */
  @PostConstruct
  public void start() {
    scheduler.scheduleAtFixedRate(
        this::refresh, 0, config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
  }

  @PreDestroy
  public void stop() {
    scheduler.shutdownNow();
  }

  public void refresh() {
    try {
      long now = redisTimeMillis();
      ZSetOperations<String, String> members = redis.opsForZSet();
      members.add(config.getKey(), instanceId, now);
      members.removeRangeByScore(
          config.getKey(), Double.NEGATIVE_INFINITY, (double) (now - config.getMemberTtlMs()));
      Long cardinality = members.zCard(config.getKey());
      if (cardinality == null) {
        // A client-level absence of an answer. Reading it as 1 is the fail-open the catch avoids.
        throw new IllegalStateException("ZCARD returned no value for " + config.getKey());
      }
      int size = Math.max(1, cardinality.intValue());
      int previous = observed.get();
      int applied;
      if (size >= previous) {
        applied = size;
        pendingShrinkSize = -1;
        pendingShrinkStreak = 0;
      } else {
        // Counted whether or not it is acted on, so a real scale-down is separable from a blip.
        observedShrinks.increment();
        if (size == pendingShrinkSize) {
          pendingShrinkStreak++;
        } else {
          pendingShrinkSize = size;
          pendingShrinkStreak = 1;
        }
        // Redis runs without persistence, so a restart returns an empty sorted set and the first
        // replica to heartbeat reads 1. That undercount is one sighting and never repeats, so
        // requiring consecutive agreement lets it self-heal while a real scale-down still lands.
        if (pendingShrinkStreak >= config.getShrinkConfirmations()) {
          applied = size;
          pendingShrinkSize = -1;
          pendingShrinkStreak = 0;
        } else {
          applied = previous;
        }
      }
      observed.set(applied);
      sized = true;
      lastRefreshMillis.set(epochMillis.getAsLong());
      consecutiveFailures.set(0);
      if (applied != previous) {
        logger.info(
            "Fleet size {} -> {}: each instance's admission floor and additive steps are now"
                + " divided by {}, so the aggregate matches what is configured",
            previous,
            applied,
            applied);
      }
    } catch (Exception e) {
      refreshFailures.increment();
      int failures = consecutiveFailures.incrementAndGet();
      // The last known size is held. Falling back to 1 fails open: every instance would take the
      // whole floor and the whole additive step, which is the pathology this class removes.
      if (failures == 1 || failures % 30 == 0) {
        if (sized) {
          logger.warn(
              "Fleet-size refresh failed ({} consecutive, holding size {} for {}s): {}. Admission"
                  + " keeps dividing by the held size deliberately, falling back to 1 would"
                  + " restore per-instance floors and step sizes across the whole fleet.",
              failures,
              observed.get(),
              String.format("%.0f", stalenessSeconds()),
              e.getMessage());
        } else {
          logger.warn(
              "Fleet-size refresh failed ({} consecutive over {}s) and this instance has never"
                  + " completed one: {}. It is not reporting ready and is taking the minimum"
                  + " admission floor until it can count the fleet, dividing by the initial size"
                  + " of 1 would give it the whole fleet-wide floor.",
              failures,
              String.format("%.0f", stalenessSeconds()),
              e.getMessage());
        }
      }
    }
  }

  /**
   * Redis server time, in epoch milliseconds. Every replica prunes every other replica's member, so
   * scoring from each instance's own clock would let a skewed one evict live instances.
   */
  private long redisTimeMillis() {
    Long time = redis.execute((RedisCallback<Long>) c -> c.serverCommands().time());
    if (time == null) {
      throw new IllegalStateException("Redis TIME returned no value");
    }
    return time;
  }
}
