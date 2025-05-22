package skiers.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import skiers.Constants;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class RateLimiter {
  private AtomicInteger capacity;
  private AtomicInteger tokens;
  private final ScheduledExecutorService scheduler;

  @Value("${rate-limiter.initial-capacity:2000}")
  private int initialCapacity;

  public RateLimiter() {
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @PostConstruct
  public void init() {
    this.capacity = new AtomicInteger(initialCapacity);
    this.tokens = new AtomicInteger(initialCapacity);
    scheduler.scheduleAtFixedRate(this::refill, 10, 10, TimeUnit.MILLISECONDS);
  }

  public synchronized void adjustRateUp(int newRate) {
    int adjustedRate = Math.min(newRate, Constants.MAX_RATE);
    capacity.set(adjustedRate);
    tokens.set(Math.min(tokens.get(), adjustedRate));
  }

  public synchronized void adjustRateDown(int newRate) {
    int adjustedRate = Math.max(newRate, Constants.MIN_RATE);
    capacity.set(adjustedRate);
    tokens.set(Math.min(tokens.get(), adjustedRate));
  }

  public boolean tryAcquire() {
    return tokens.getAndUpdate(t -> t > 0 ? t - 1 : t) > 0;
  }

  private void refill() {
    int currentCapacity = capacity.get();
    int tokensToAdd = (currentCapacity * 10) / 1000; // 10ms interval
    tokens.getAndUpdate(t -> Math.min(t + tokensToAdd, currentCapacity));
  }

  public int getCurrentRate() {
    return capacity.get();
  }
}