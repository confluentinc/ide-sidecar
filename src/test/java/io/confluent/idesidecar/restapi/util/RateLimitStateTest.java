package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RateLimitStateTest {

  private static final Instant NOW = Instant.parse("2026-11-06T12:00:00Z");

  @ParameterizedTest
  @CsvSource({
      "0, 0",
      "30, 30",
      "' 7 ', 7",
      "99999999999, 2147483647",
      "00000000001, 1",
      "000000000000, 0",
  })
  void shouldParseDelaySeconds(String header, int expected) {
    assertEquals(expected, RateLimitState.parseRetryAfter(header, NOW));
  }

  @Test
  void shouldParseHttpDateAsSecondsFromNow() {
    var header = "Fri, 06 Nov 2026 12:00:45 GMT";

    var seconds = RateLimitState.parseRetryAfter(header, NOW);

    assertEquals(45, seconds);
  }

  @Test
  void shouldRoundPartialSecondsOfHttpDateUp() {
    var halfSecondBefore = NOW.minusMillis(500);

    var seconds = RateLimitState.parseRetryAfter("Fri, 06 Nov 2026 12:00:10 GMT", halfSecondBefore);

    assertEquals(11, seconds);
  }

  @Test
  void shouldTreatPastHttpDateAsNoWait() {
    var seconds = RateLimitState.parseRetryAfter("Fri, 06 Nov 2026 11:59:00 GMT", NOW);

    assertEquals(0, seconds);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"soon", "-5", "1.5", "Fri, 06 Nov 2026"})
  void shouldReturnMinusOneForAbsentOrInvalidValues(String header) {
    assertEquals(-1, RateLimitState.parseRetryAfter(header, NOW));
  }
}
