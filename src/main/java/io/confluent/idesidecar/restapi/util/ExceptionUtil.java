package io.confluent.idesidecar.restapi.util;

import io.netty.channel.ConnectTimeoutException;
import java.util.concurrent.TimeoutException;

/**
 * Utilities related to exception handling.
 */
public final class ExceptionUtil {

  private ExceptionUtil() {
  }

  /**
   * Extract the root cause of a throwable recursively.
   *
   * @param t the throwable
   * @return the root cause
   */
  public static Throwable unwrap(Throwable t) {
    if (t.getCause() != null) {
      return unwrap(t.getCause());
    }
    return t;
  }

  /**
   * The first throwable of {@code type} in {@code t}'s cause chain, starting with {@code t}, or
   * null if there is none.
   */
  public static <T extends Throwable> T findCause(Throwable t, Class<T> type) {
    for (var cause = t; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return type.cast(cause);
      }
    }
    return null;
  }

  /**
   * Whether {@code t}, or anything in its cause chain, is a request or connect timeout.
   */
  public static boolean isTimeout(Throwable t) {
    // Netty's connect timeout is a ConnectException, but a refused connection is too, so match
    // the timeout's own type
    return findCause(t, TimeoutException.class) != null
        || findCause(t, ConnectTimeoutException.class) != null;
  }

  public static Throwable unwrapWithCombinedMessage(Throwable t) {
    if (t.getCause() != null) {
      return new Throwable(
          t.getMessage()
              + " caused by: "
              + unwrapWithCombinedMessage(t.getCause()).getMessage(),
          t.getCause()
      );
    }
    return t;
  }
}
