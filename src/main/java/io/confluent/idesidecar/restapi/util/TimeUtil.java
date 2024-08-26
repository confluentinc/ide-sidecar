package io.confluent.idesidecar.restapi.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeUtil {

  public static String humanReadableDuration(Duration duration) {
    var builder = new StringBuilder();
    var days = duration.toDays();
    if (days != 0) {
      appendSegment(builder, days, "d");
    }
    var hours = duration.toHoursPart();
    if (hours != 0) {
      appendSegment(builder, hours, "h");
    }
    var minutes = duration.toMinutesPart();
    if (minutes != 0) {
      appendSegment(builder, minutes, "m");
    }
    var seconds = duration.toSecondsPart();
    var millis = TimeUnit.NANOSECONDS.toMillis(duration.getNano());
    if (seconds != 0 || millis != 0) {
      var decimalSeconds = Long.toString(seconds);
      if (millis > 0) {
        decimalSeconds = "%s.%03d".formatted(decimalSeconds, millis).replaceAll("(0{1,2})$", "");
      }
      appendSegment(builder, decimalSeconds, "s");
    }
    return builder.toString();
  }

  private static void appendSegment(StringBuilder sb, Object value, String unit) {
    if (sb.length() > 0) {
      sb.append(" ");
    }
    sb.append(value).append(unit);
  }
}
