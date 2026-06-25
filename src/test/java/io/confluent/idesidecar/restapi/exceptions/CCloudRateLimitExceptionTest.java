package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CCloudRateLimitExceptionTest {

  @Test
  void shouldIncludeUrlAndRetryAfterInMessage() {
    var ex = new CCloudRateLimitException("https://api.confluent.cloud/api/org/v2/environments", 5);

    assertTrue(ex.getMessage().contains("environments"));
    assertTrue(ex.getMessage().contains("5s"));
    assertEquals(5, ex.getRetryAfterSeconds());
  }

  @Test
  void shouldDefaultRetryAfterToNegativeOne() {
    var ex = new CCloudRateLimitException("https://api.confluent.cloud/api/org/v2/environments");

    assertEquals(-1, ex.getRetryAfterSeconds());
    assertTrue(ex.getMessage().contains("-1s"));
  }

  @Test
  void shouldBeRuntimeException() {
    var ex = new CCloudRateLimitException("https://example.com");

    assertTrue(ex instanceof RuntimeException);
  }
}
