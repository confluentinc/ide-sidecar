package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.testutil.ShortWebClientTimeoutProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PfxOptions;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A request's timeout caps the whole exchange (getting a connection, the TLS handshake, and the
 * response) and not just each silence, and the request is cancelled when it runs out.
 */
@QuarkusTest
@TestProfile(ShortWebClientTimeoutProfile.class)
class WebClientDeadlineTest {

  /** Each phase stays under the 1s timeout, but together they run past it. */
  private static final long HANDSHAKE_HOLD_MS = 600;
  private static final long RESPONSE_DELAY_MS = 900;
  /** Each hop of a redirect stays under the 1s timeout, but the two together run past it. */
  private static final long HOP_DELAY_MS = 600;
  /** How soon after the timeout the request must have failed. */
  private static final long GIVE_UP_WITHIN_MS =
      ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS + 1_000;
  /** Long enough for any deadline left behind by an earlier request to fire. */
  private static final long PAST_THE_TIMEOUT_MS =
      ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS + 500;
  /** Gaps between body chunks, well under the 1s timeout. */
  private static final long TRICKLE_INTERVAL_MS = 200;
  /** The trickle ends after this, so a client without an overall cap gets the whole body. */
  private static final long TRICKLE_FOR_MS = ShortWebClientTimeoutProfile.STALL_MS;
  private static final String KEYSTORE_PASSWORD = "changeit";

  @TempDir
  static Path keyStoreDir;

  private static Path keyStore;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  Vertx vertx;

  private HttpServer server;
  private NetServer relay;
  private NetClient relayClient;
  private final Promise<Void> serverConnectionClosed = Promise.promise();

  @BeforeAll
  static void createKeyStore() throws Exception {
    keyStore = keyStoreDir.resolve("localhost.p12");
    var keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    var process = new ProcessBuilder(
        keytool, "-genkeypair", "-alias", "localhost", "-keyalg", "RSA", "-keysize", "2048",
        "-dname", "CN=localhost", "-validity", "1", "-storetype", "PKCS12",
        "-keystore", keyStore.toString(),
        "-storepass", KEYSTORE_PASSWORD, "-keypass", KEYSTORE_PASSWORD
    ).redirectErrorStream(true).start();
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "keytool didn't finish");
    assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes()));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (relay != null) {
      await(relay.close());
      await(relayClient.close());
    }
    if (server != null) {
      await(server.close());
    }
  }

  @Test
  void proxyShouldGiveUpAtTheTimeoutWhenTheHandshakeAndResponseTogetherRunPastIt()
      throws Exception {
    var backendPort = startServer(
        new HttpServerOptions().setSsl(true)
            .setKeyCertOptions(new PfxOptions().setPath(keyStore.toString())
                .setPassword(KEYSTORE_PASSWORD)),
        request -> {
          if (request.path().equals("/warm-up")) {
            request.response().end("ok");
          } else {
            watchConnection(request);
            vertx.setTimer(RESPONSE_DELAY_MS, id -> request.response().end("ok"));
          }
        }
    );
    // the first TLS setup in a JVM is slow, so warm it up without the relay
    await(new ProxyHttpClient<>(webClientFactory, vertx).send(
        tlsProxyContext("https://localhost:%d/warm-up".formatted(backendPort)),
        Duration.ofSeconds(10)
    ));
    var relayPort = startHandshakeHoldingRelay(backendPort);
    var context = tlsProxyContext("https://localhost:%d/slow".formatted(relayPort));
    var started = System.nanoTime();

    var failure = awaitFailure(new ProxyHttpClient<>(webClientFactory, vertx).send(context));

    assertProxyTimeout(failure);
    assertGaveUpAtTheTimeout(started);
    await(serverConnectionClosed.future());
  }

  @Test
  void proxyShouldGiveUpAtTheTimeoutWhenTheResponseTrickles() throws Exception {
    var context = proxyContext("http://localhost:%d/trickle".formatted(startTrickleServer()));
    var started = System.nanoTime();

    var failure = awaitFailure(new ProxyHttpClient<>(webClientFactory, vertx).send(context));

    assertProxyTimeout(failure);
    assertGaveUpAtTheTimeout(started);
    await(serverConnectionClosed.future());
  }

  @Test
  void proxyShouldKeepTheDeadlineAfterPreferencesRebuildTheWebClient() throws Exception {
    webClientFactory.updateWebClientOptions(new PreferencesSpec(null, null, false, null));
    var context = proxyContext("http://localhost:%d/trickle".formatted(startTrickleServer()));
    var started = System.nanoTime();

    var failure = awaitFailure(new ProxyHttpClient<>(webClientFactory, vertx).send(context));

    assertProxyTimeout(failure);
    assertGaveUpAtTheTimeout(started);
  }

  @Test
  void proxyShouldGiveUpAtTheTimeoutWhenRedirectsTogetherRunPastIt() throws Exception {
    var port = startServer(new HttpServerOptions(), request -> vertx.setTimer(HOP_DELAY_MS, id -> {
      if (request.path().equals("/first-hop")) {
        request.response().setStatusCode(302).putHeader("Location", "/second-hop").end();
      } else {
        request.response().end("ok");
      }
    }));
    var context = proxyContext("http://localhost:%d/first-hop".formatted(port));
    var started = System.nanoTime();

    var failure = awaitFailure(new ProxyHttpClient<>(webClientFactory, vertx).send(context));

    assertProxyTimeout(failure);
    assertGaveUpAtTheTimeout(started);
  }

  @Test
  void proxyShouldLeaveTheConnectionOpenAfterAQuickResponse() throws Exception {
    var connectionsOpened = new AtomicInteger();
    server = vertx.createHttpServer()
        .connectionHandler(connection -> {
          connectionsOpened.incrementAndGet();
          connection.closeHandler(v -> serverConnectionClosed.tryComplete());
        })
        .requestHandler(request -> request.response().end("ok"));
    var port = await(server.listen(0, "localhost")).actualPort();
    var url = "http://localhost:%d/quick".formatted(port);
    var client = new ProxyHttpClient<>(webClientFactory, vertx);
    await(client.send(proxyContext(url)));
    Thread.sleep(PAST_THE_TIMEOUT_MS);

    var second = await(client.send(proxyContext(url)));

    assertEquals(200, second.getProxyResponseStatusCode());
    assertEquals(1, connectionsOpened.get(), "the first request's connection was closed");
    assertFalse(serverConnectionClosed.future().isComplete());
  }

  /** Start a server that sends its body a byte at a time, for longer than the timeout. */
  private int startTrickleServer() throws Exception {
    return startServer(new HttpServerOptions(), request -> {
      watchConnection(request);
      var response = request.response().setChunked(true);
      var trickleUntil = System.currentTimeMillis() + TRICKLE_FOR_MS;
      vertx.setPeriodic(TRICKLE_INTERVAL_MS, id -> {
        if (response.closed() || response.ended()) {
          vertx.cancelTimer(id);
        } else if (System.currentTimeMillis() >= trickleUntil) {
          vertx.cancelTimer(id);
          response.end();
        } else {
          response.write("x");
        }
      });
    });
  }

  private int startServer(
      HttpServerOptions options,
      Handler<HttpServerRequest> handler
  ) throws Exception {
    server = vertx.createHttpServer(options).requestHandler(handler);
    return await(server.listen(0, "localhost")).actualPort();
  }

  /** Records when the connection serving {@code request} closes. */
  private void watchConnection(HttpServerRequest request) {
    request.connection().closeHandler(v -> serverConnectionClosed.tryComplete());
  }

  /** A TCP relay that holds each new connection before passing bytes, stalling the handshake. */
  private int startHandshakeHoldingRelay(int backendPort) throws Exception {
    relayClient = vertx.createNetClient();
    relay = vertx.createNetServer().connectHandler(inbound -> {
      inbound.pause();
      vertx.setTimer(HANDSHAKE_HOLD_MS, id -> relayClient.connect(backendPort, "localhost")
          .onSuccess(outbound -> pipe(inbound, outbound))
          .onFailure(e -> inbound.close()));
    });
    return await(relay.listen(0, "localhost")).actualPort();
  }

  private static void pipe(NetSocket inbound, NetSocket outbound) {
    inbound.handler(outbound::write);
    outbound.handler(inbound::write);
    inbound.closeHandler(v -> outbound.close());
    outbound.closeHandler(v -> inbound.close());
    inbound.resume();
  }

  private static ProxyContext tlsProxyContext(String url) {
    var context = proxyContext(url);
    context.setWebClientOptions(
        new WebClientOptions().setSsl(true).setTrustAll(true).setVerifyHost(false)
    );
    return context;
  }

  private static ProxyContext proxyContext(String url) {
    var context = new ProxyContext(
        url, MultiMap.caseInsensitiveMultiMap(), HttpMethod.GET, null, Map.of(), null
    );
    context.setProxyRequestMethod(HttpMethod.GET);
    context.setProxyRequestAbsoluteUrl(url);
    context.setProxyRequestHeaders(MultiMap.caseInsensitiveMultiMap());
    context.setProxyRequestBody(Buffer.buffer());
    return context;
  }

  private static void assertProxyTimeout(Throwable failure) {
    var processorFailure = assertInstanceOf(ProcessorFailedException.class, failure);
    assertEquals("504", processorFailure.getFailure().status());
    assertEquals("proxy_timeout", processorFailure.getFailure().errors().get(0).code());
    assertEquals(
        "localhost didn't respond within 1s", processorFailure.getFailure().title()
    );
  }

  private static void assertGaveUpAtTheTimeout(long startedNanos) {
    var elapsed = Duration.ofNanos(System.nanoTime() - startedNanos);
    assertTrue(
        elapsed.toMillis() >= ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS,
        "gave up before the timeout: " + elapsed
    );
    assertTrue(elapsed.toMillis() < GIVE_UP_WITHIN_MS, "took " + elapsed);
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private static Throwable awaitFailure(Future<?> future) throws Exception {
    var failure = future
        .map(result -> (Throwable) new AssertionError("expected a failure, got " + result))
        .recover(Future::succeededFuture);
    return await(failure);
  }
}
