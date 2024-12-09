package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TimeUtilTest {

  @Test
  public void testHumanReadableDuration() {
    assertReadable("0.1s", 0, 0, 0, 0, 100);
    assertReadable("12s", 0, 0, 0, 12, 0);
    assertReadable("12.012s", 0, 0, 0, 12, 12);
    assertReadable("59m 12.105s", 0, 0, 59, 12, 105);
    assertReadable("2h 59m 12.001s", 0, 2, 59, 12, 1);
    assertReadable("3d 2h 59m 12.999s", 3, 2, 59, 12, 999);
    assertReadable("59m 12s", 0, 0, 59, 12, 0);
  }

  protected void assertReadable(
      String expected,
      int days,
      int hours,
      int minutes,
      int seconds,
      int millis
  ) {
    var duration = Duration.ofMillis(
        millis
            + TimeUnit.SECONDS.toMillis(seconds)
            + TimeUnit.MINUTES.toMillis(minutes)
            + TimeUnit.HOURS.toMillis(hours)
            + TimeUnit.DAYS.toMillis(days)
    );
    assertEquals(
        expected,
        TimeUtil.humanReadableDuration(duration)
    );
  }
}