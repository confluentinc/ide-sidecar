package io.confluent.idesidecar.restapi.util;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

/**
 * Utility class to execute a retryable operation with a configurable number of retries and backoff
 * strategy.
 */
public class RetryableExecutor {
  private static final int DEFAULT_MAX_RETRIES = 5;
  private static final Duration DEFAULT_RETRY_DELAY = Duration.ofMillis(500);
  private static final Duration DEFAULT_MAX_RETRY_DELAY = Duration.ofSeconds(5);

  private static final RetryableExecutor DEFAULT = new RetryableExecutor();

  private final int maxRetries;
  private final Duration retryDelay;
  private final Duration maxRetryDelay;
  private final BackoffStrategy backoffStrategy;
  private final Sleeper sleeper;
  private final Clock clock;

  public RetryableExecutor() {
    this(
        DEFAULT_MAX_RETRIES,
        DEFAULT_RETRY_DELAY,
        DEFAULT_MAX_RETRY_DELAY,
        BackoffStrategy.EXPONENTIAL
    );
  }

  public RetryableExecutor(
      int maxRetries,
      Duration retryDelay,
      Duration maxRetryDelay,
      BackoffStrategy backoffStrategy
  ) {
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
    this.maxRetryDelay = maxRetryDelay;
    this.backoffStrategy = backoffStrategy;
    this.sleeper = new DefaultSleeper();
    this.clock = Instant::now;
  }

  public RetryableExecutor(
      int maxRetries,
      Duration retryDelay,
      Duration maxRetryDelay,
      BackoffStrategy backoffStrategy,
      Sleeper sleeper,
      Clock clock
  ) {
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
    this.maxRetryDelay = maxRetryDelay;
    this.backoffStrategy = backoffStrategy;
    this.sleeper = sleeper;
    this.clock = clock;
  }

  public static <T> T executeWithRetry(
      Retryable<T> retryable,
      Predicate<Exception> retryPredicate
  ) throws Exception {
    return DEFAULT.execute(retryable, retryPredicate);
  }

  /**
   * Execute a retryable operation with a configurable number of retries and backoff strategy.
   * Note that this method will block the current thread until the operation succeeds or the maximum
   * number of retries is reached, so please exercise caution when using this method in an
   * event-loop thread or other non-blocking context.
   * @param retryable      The operation to execute.
   * @param retryPredicate A predicate to determine if the operation should be retried based on the
   *                       exception thrown.
   * @return               The result of the operation.
   * @param <T>            The return type of the operation.
   * @throws Exception     If the operation fails after the maximum number of retries.
   */
  public final <T> T execute(
      Retryable<T> retryable,
      Predicate<Exception> retryPredicate
  ) throws Exception {
    var retries = 0;
    var startTime = clock.now();
    while (true) {
      try {
        return retryable.execute();
      } catch (Exception e) {
        var elapsedTime = Duration.between(startTime, clock.now());
        if (
            // Order is very important here
            // If the elapsed time is greater than the maximum retry delay
            elapsedTime.compareTo(maxRetryDelay) >= 0
                // Or if we've reached the maximum number of retries
                || retries >= maxRetries
                // Or the exception is not retryable
                || !retryPredicate.test(e)
        ) {
          throw e;
        }
        retries++;
        try {
          var backoffDelay = calculateBackoffDelay(retries);
          sleeper.sleep(backoffDelay);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
  }

  /**
   * Calculate the backoff delay for the given number of retries. This method is package-private for
   * testing purposes.
   */
  protected long calculateBackoffDelay(int retries) {
    long delay;
    if (backoffStrategy == BackoffStrategy.LINEAR) {
      delay = retryDelay.toMillis() * retries;
    } else if (backoffStrategy == BackoffStrategy.EXPONENTIAL) {
      delay = (long) (retryDelay.toMillis() * Math.pow(2, retries - 1));
    } else {
      throw new IllegalStateException("Unknown backoff strategy: " + backoffStrategy);
    }
    return Math.min(delay, maxRetryDelay.toMillis());
  }

  public enum BackoffStrategy {
    LINEAR,
    EXPONENTIAL
  }

  public interface Retryable<T> {
    T execute() throws Exception;
  }

  public static class DefaultSleeper implements Sleeper {
    @Override
    public void sleep(long millis) throws InterruptedException {
      Thread.sleep(millis);
    }
  }

  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  public interface Clock {
    Instant now();
  }
}