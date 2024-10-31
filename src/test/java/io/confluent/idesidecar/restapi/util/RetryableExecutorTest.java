package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

class RetryableExecutorTest {
  final static Predicate<Exception> RETRY_PREDICATE = e -> true;
  final static RetryableExecutor.Retryable<String> EXCEPTION_SUPPLIER = () -> {
    throw new Exception("retry");
  };

  final static RetryableExecutor.Retryable<String> SUCCESS_SUPPLIER = () -> "success";

  @Nested
  class RetryableExecutorWithExponentialBackoff {
    private RetryableExecutor.Sleeper mockSleeper;
    private RetryableExecutor.Clock mockClock;
    private RetryableExecutor executor;

    @BeforeEach
    void setUp() {
      mockSleeper = mock(RetryableExecutor.Sleeper.class);
      mockClock = mock(RetryableExecutor.Clock.class);
      executor = new RetryableExecutor(
          3,
          Duration.ofMillis(500),
          Duration.ofSeconds(5),
          RetryableExecutor.BackoffStrategy.EXPONENTIAL,
          mockSleeper,
          mockClock
      );
    }

    @Test
    void testSuccessfulExecution() throws Exception {
      when(mockClock.now()).thenReturn(Instant.now());

      var result = executor.execute(SUCCESS_SUPPLIER, RETRY_PREDICATE);

      assertEquals("success", result);
      verify(mockSleeper, never()).sleep(anyLong());
    }

    @Test
    void testRetryAndSuccess() throws Exception {
      var attempts = new AtomicInteger(0);
      RetryableExecutor.Retryable<String> retryable = () -> {
        if (attempts.incrementAndGet() < 3) {
          throw new Exception("retry");
        }
        return "success";
      };

      when(mockClock.now()).thenReturn(Instant.now());
      var result = executor.execute(retryable, RETRY_PREDICATE);
      assertEquals("success", result);
      verify(mockSleeper, times(2)).sleep(anyLong());
    }

    @Test
    void testMaxRetriesExceeded() throws Exception {
      when(mockClock.now()).thenReturn(Instant.now());
      var exception = assertThrows(Exception.class, () -> executor.execute(
          EXCEPTION_SUPPLIER, RETRY_PREDICATE)
      );
      assertEquals("retry", exception.getMessage());
      verify(mockSleeper, times(3)).sleep(anyLong());
    }

    @Test
    void testMaxRetryDelayExceeded() throws Exception {
      when(mockClock.now()).thenReturn(
          Instant.now(),
          Instant.now().plusMillis(1000),
          Instant.now().plusMillis(2000)
      );
      executor = new RetryableExecutor(
          10,
          Duration.ofMillis(500),
          Duration.ofMillis(1000),
          RetryableExecutor.BackoffStrategy.EXPONENTIAL,
          mockSleeper,
          mockClock
      );
      var exception = assertThrows(
          Exception.class, () -> executor.execute(EXCEPTION_SUPPLIER, RETRY_PREDICATE)
      );
      assertEquals("retry", exception.getMessage());
      verify(mockSleeper, atMost(2)).sleep(anyLong());
    }

    @Test
    void testInterruptedException() throws Exception {
      when(mockClock.now()).thenReturn(Instant.now());
      doThrow(new InterruptedException()).when(mockSleeper).sleep(anyLong());
      var exception = assertThrows(RuntimeException.class,
          () -> executor.execute(EXCEPTION_SUPPLIER, RETRY_PREDICATE)
      );
      assertInstanceOf(InterruptedException.class, exception.getCause());
      verify(mockSleeper, times(1)).sleep(anyLong());
    }

    @Test
    void testExponentialBackoffDelay() throws Exception {
      assertEquals(500, executor.calculateBackoffDelay(1));
      assertEquals(1000, executor.calculateBackoffDelay(2));
      assertEquals(2000, executor.calculateBackoffDelay(3));
      assertEquals(4000, executor.calculateBackoffDelay(4));
      // Max delay is 5 seconds
      assertEquals(5000, executor.calculateBackoffDelay(5));
      assertEquals(5000, executor.calculateBackoffDelay(6));
    }
  }

  @Nested
  class RetryableExecutorWithLinearBackoff {
    private RetryableExecutor.Sleeper mockSleeper;
    private RetryableExecutor.Clock mockClock;
    private RetryableExecutor executor;

    @BeforeEach
    void setUp() {
      mockSleeper = mock(RetryableExecutor.Sleeper.class);
      mockClock = mock(RetryableExecutor.Clock.class);
      executor = new RetryableExecutor(
          3,
          Duration.ofMillis(500),
          Duration.ofSeconds(5),
          RetryableExecutor.BackoffStrategy.LINEAR,
          mockSleeper,
          mockClock
      );
    }

    @Test
    void testSuccessfulExecution() throws Exception {
      when(mockClock.now()).thenReturn(Instant.now());
      var result = executor.execute(SUCCESS_SUPPLIER, RETRY_PREDICATE);
      assertEquals("success", result);
      verify(mockSleeper, never()).sleep(anyLong());
    }

    @Test
    void testRetryAndSuccess() throws Exception {
      var attempts = new AtomicInteger(0);
      RetryableExecutor.Retryable<String> retryable = () -> {
        if (attempts.incrementAndGet() < 3) {
          throw new Exception("retry");
        }
        return "success";
      };
      when(mockClock.now()).thenReturn(Instant.now());
      var result = executor.execute(retryable, RETRY_PREDICATE);
      assertEquals("success", result);
      verify(mockSleeper, times(2)).sleep(anyLong());
    }

    @Test
    void testMaxRetriesExceeded() throws Exception {
      when(mockClock.now()).thenReturn(Instant.now());
      var exception = assertThrows(Exception.class, () ->
          executor.execute(EXCEPTION_SUPPLIER, RETRY_PREDICATE)
      );
      assertEquals("retry", exception.getMessage());
      verify(mockSleeper, times(3)).sleep(anyLong());
    }

    @Test
    void testMaxRetryDelayExceeded() throws Exception {
      Predicate<Exception> retryPredicate = e -> e.getMessage().equals("retry");
      when(mockClock.now())
          .thenReturn(
              Instant.now(),
              Instant.now().plusMillis(1000),
              Instant.now().plusMillis(2000)
          );
      executor = new RetryableExecutor(
          10,
          Duration.ofMillis(500),
          Duration.ofMillis(1000),
          RetryableExecutor.BackoffStrategy.LINEAR,
          mockSleeper,
          mockClock
      );
      var exception = assertThrows(
          Exception.class, () -> executor.execute(EXCEPTION_SUPPLIER, retryPredicate)
      );
      assertEquals("retry", exception.getMessage());
      verify(mockSleeper, atMost(2)).sleep(anyLong());
    }

    @Test
    void testLinearBackoffDelay() {
      assertEquals(500, executor.calculateBackoffDelay(1));
      assertEquals(1000, executor.calculateBackoffDelay(2));
      assertEquals(1500, executor.calculateBackoffDelay(3));
      assertEquals(2000, executor.calculateBackoffDelay(4));
      assertEquals(2500, executor.calculateBackoffDelay(5));

      // Max delay is 5 seconds
      assertEquals(5000, executor.calculateBackoffDelay(11));
      assertEquals(5000, executor.calculateBackoffDelay(12));
    }
  }
}