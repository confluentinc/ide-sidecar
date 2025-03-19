package io.confluent.idesidecar.restapi.util;

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

  public static Throwable unwrapWithCombinedMessage(Throwable t) {
    if (t.getCause() != null) {
      return new Throwable(
          t.getMessage() + " caused by: " + unwrapWithCombinedMessage(t.getCause()),
          t.getCause()
      );
    }
    return t;
  }
}
