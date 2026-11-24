package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.ssl.SslHandshakeTimeoutException;
import io.netty.resolver.dns.DnsNameResolverTimeoutException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class ExceptionUtilTest {

  private static final String HOST = "api.confluent.cloud";

  @Test
  void isTimeoutShouldMatchAWrappedSocketTimeout() {
    var failure = new CompletionException(new SocketTimeoutException("Read timed out"));

    assertTrue(ExceptionUtil.isTimeout(failure));
  }

  @Test
  void isTimeoutShouldMatchADnsTimeout() {
    assertTrue(ExceptionUtil.isTimeout(new CompletionException(dnsTimeout())));
  }

  @Test
  void isTimeoutShouldNotMatchRefusedConnectionsUnknownHostsOrTlsFailures() {
    assertFalse(ExceptionUtil.isTimeout(new ConnectException("Connection refused")));
    assertFalse(ExceptionUtil.isTimeout(new UnknownHostException(HOST)));
    assertFalse(ExceptionUtil.isTimeout(new SSLHandshakeException("bad certificate")));
  }

  @Test
  void isConnectTimeoutShouldMatchConnectDnsAndTlsHandshakeTimeouts() {
    assertTrue(ExceptionUtil.isConnectTimeout(
        new CompletionException(new ConnectTimeoutException("connection timed out"))
    ));
    assertTrue(ExceptionUtil.isConnectTimeout(new CompletionException(dnsTimeout())));
    assertTrue(ExceptionUtil.isConnectTimeout(new CompletionException(
        new SslHandshakeTimeoutException("handshake timed out after 10000ms")
    )));
  }

  @Test
  void isConnectTimeoutShouldNotMatchRequestTimeoutsOrOtherDnsFailures() {
    var unresolvable = new UnknownHostException(HOST);
    unresolvable.initCause(new IOException("NXDOMAIN"));

    assertFalse(ExceptionUtil.isConnectTimeout(new TimeoutException("no response in 20s")));
    assertFalse(ExceptionUtil.isConnectTimeout(unresolvable));
  }

  /**
   * A DNS lookup that timed out, as Netty reports it: the resolver's timeout under an
   * {@link UnknownHostException}.
   */
  private static UnknownHostException dnsTimeout() {
    var failure = new UnknownHostException("Failed to resolve '" + HOST + "'");
    failure.initCause(new DnsNameResolverTimeoutException(
        new InetSocketAddress("127.0.0.1", 53),
        new DefaultDnsQuestion(HOST, DnsRecordType.A),
        "query via UDP timed out after 5000 milliseconds"
    ));
    return failure;
  }
}
