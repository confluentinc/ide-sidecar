package io.confluent.idesidecar.restapi.util;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.ssl.SslHandshakeTimeoutException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
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
   * Whether {@code t} itself is a timeout: a request, socket, connect, DNS lookup or TLS handshake
   * timeout. A refused connection, an unknown host or another TLS failure isn't one. Netty's
   * connect timeout extends {@link java.net.ConnectException}, like a refused connection, so it's
   * matched by its own type, and its DNS and TLS handshake timeouts aren't
   * {@link TimeoutException}s, hence the name check.
   *
   * @see <a href="https://github.com/netty/netty/blob/netty-4.1.127.Final/transport/src/main/java/io/netty/channel/ConnectTimeoutException.java#L24">ConnectTimeoutException</a>
   * @see <a href="https://github.com/netty/netty/blob/netty-4.1.127.Final/resolver-dns/src/main/java/io/netty/resolver/dns/DnsResolveContext.java#L1150-L1156">DNS timeout wrapped in UnknownHostException</a>
   */
  public static boolean isTimeoutType(Throwable t) {
    return t instanceof TimeoutException
        || t instanceof SocketTimeoutException
        || t instanceof ConnectTimeoutException
        || t.getClass().getSimpleName().endsWith("TimeoutException");
  }

  /**
   * The first timeout (see {@link #isTimeoutType}) in {@code t}'s cause chain, starting with
   * {@code t}, or null if there is none.
   */
  public static Throwable findTimeout(Throwable t) {
    for (var cause = t; cause != null; cause = cause.getCause()) {
      if (isTimeoutType(cause)) {
        return cause;
      }
    }
    return null;
  }

  /**
   * Whether {@code t}, or anything in its cause chain, is a timeout (see {@link #isTimeoutType}).
   */
  public static boolean isTimeout(Throwable t) {
    return findTimeout(t) != null;
  }

  /**
   * Whether {@code t} timed out before a connection was made: a connect timeout, a DNS lookup that
   * timed out, or a TLS handshake that timed out.
   */
  public static boolean isConnectTimeout(Throwable t) {
    var unknownHost = findCause(t, UnknownHostException.class);
    return findCause(t, ConnectTimeoutException.class) != null
        || findCause(t, SslHandshakeTimeoutException.class) != null
        || (unknownHost != null && isTimeout(unknownHost));
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
