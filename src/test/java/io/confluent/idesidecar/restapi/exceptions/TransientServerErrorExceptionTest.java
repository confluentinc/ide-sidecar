package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TransientServerErrorExceptionTest {

  private static final String URL = "https://api.confluent.cloud/org/v2/environments";

  @Test
  void messageShouldIncludeRetryAfterWhenTheResponseHadOne() {
    var exception = new TransientServerErrorException(URL, 503, 30, new RuntimeException("503"));

    assertEquals("HTTP 503 from " + URL + " (retry-after: 30s)", exception.getMessage());
  }

  @Test
  void messageShouldLeaveOutRetryAfterWhenTheResponseHadNone() {
    var exception = new TransientServerErrorException(URL, 502, -1, new RuntimeException("502"));

    assertEquals("HTTP 502 from " + URL, exception.getMessage());
  }
}
