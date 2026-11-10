package io.confluent.idesidecar.restapi.util;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.testutil.ShortWebClientTimeoutProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Each outbound WebClient call site gives up on a server that stops answering, after the
 * configured request timeout instead of waiting forever.
 */
@QuarkusTest
@ConnectWireMock
@TestProfile(ShortWebClientTimeoutProfile.class)
class WebClientRequestTimeoutTest {

  private static final String FAKE_AUTHORIZATION_CODE = "fake_authorization_code";
  private static final String CHECK_JWT_BODY = "{\"error\":null}";

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  Vertx vertx;

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  CCloudApiRateLimiter rateLimiter;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;

  @BeforeEach
  void setUp() {
    new CCloudTestUtil(wireMock, connectionStateManager)
        .registerWireMockRoutesForCCloudOAuth(FAKE_AUTHORIZATION_CODE, null, null);
    rateLimiter.reset();
  }

  @AfterEach
  void tearDown() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.removeMappings();
  }

  @Test
  void proxyShouldFailWithGatewayTimeoutWhenUpstreamStalls() throws Exception {
    stall(WireMock.get("/stalled"));
    var context = new ProxyContext(
        "/stalled", MultiMap.caseInsensitiveMultiMap(), HttpMethod.GET, null, Map.of(), null
    );
    context.setProxyRequestMethod(HttpMethod.GET);
    context.setProxyRequestAbsoluteUrl("http://localhost:%d/stalled".formatted(wireMockPort));
    context.setProxyRequestHeaders(MultiMap.caseInsensitiveMultiMap());
    context.setProxyRequestBody(Buffer.buffer());
    var started = System.nanoTime();

    var failure = awaitFailure(new ProxyHttpClient<>(webClientFactory, vertx).send(context));

    assertGaveUpInTime(started);
    var processorFailure = assertInstanceOf(ProcessorFailedException.class, failure);
    assertEquals("504", processorFailure.getFailure().status());
    assertEquals("proxy_timeout", processorFailure.getFailure().errors().get(0).code());
  }

  @Test
  void proxyShouldWaitForTheLongerConsumeTimeoutWhenGivenOne() throws Exception {
    stall(WireMock.post("/stalled-consume"));
    var context = new ProxyContext(
        "/stalled-consume",
        MultiMap.caseInsensitiveMultiMap(),
        HttpMethod.POST,
        null,
        Map.of(),
        null
    );
    context.setProxyRequestMethod(HttpMethod.POST);
    context.setProxyRequestAbsoluteUrl(
        "http://localhost:%d/stalled-consume".formatted(wireMockPort)
    );
    context.setProxyRequestHeaders(MultiMap.caseInsensitiveMultiMap());
    context.setProxyRequestBody(Buffer.buffer("{}"));
    var started = System.nanoTime();

    var failure = awaitFailure(
        new ProxyHttpClient<>(webClientFactory, vertx)
            .send(context, WebClientFactory.WEBCLIENT_CCLOUD_CONSUME_TIMEOUT_SECONDS)
    );

    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(
        elapsed.toMillis() >= ShortWebClientTimeoutProfile.CONSUME_TIMEOUT_MS,
        "gave up before the consume timeout: " + elapsed
    );
    assertTrue(
        elapsed.toMillis() < ShortWebClientTimeoutProfile.GIVE_UP_WITHIN_MS,
        "took " + elapsed
    );
    assertInstanceOf(ProcessorFailedException.class, failure);
  }

  @Test
  void loginRealmShouldFailWhenCCloudStalls() {
    stall(WireMock.get(WireMock.urlPathEqualTo("/api/login/realm")));
    var started = System.nanoTime();

    var status = given()
        .queryParam("email", "someone@example.com")
        .when()
        .get("/api/login/realm")
        .then()
        .extract()
        .statusCode();

    assertGaveUpInTime(started);
    assertEquals(504, status);
  }

  @Test
  void checkJwtShouldFailWhenCCloudStalls() throws Exception {
    var authContext = new CCloudOAuthContext();
    awaitResult(authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null));
    stall(WireMock.get("/api/check_jwt").atPriority(1));
    var started = System.nanoTime();

    var failure = awaitFailure(authContext.checkAuthenticationStatus());

    assertGaveUpInTime(started);
    assertInstanceOf(TimeoutException.class, failure);
    assertTrue(
        authContext.getErrors().authStatusCheck().message().contains("timeout"),
        authContext.getErrors().authStatusCheck().message()
    );
  }

  @Test
  void checkJwtShouldTimeOutWhenCCloudTricklesItsAnswerPastTheTimeout() throws Exception {
    var authContext = new CCloudOAuthContext();
    awaitResult(authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null));
    wireMock.register(WireMock.get("/api/check_jwt").atPriority(1).willReturn(
        WireMock.aResponse().withStatus(200).withBody(CHECK_JWT_BODY).withChunkedDribbleDelay(
            CHECK_JWT_BODY.length(), ShortWebClientTimeoutProfile.STALL_MS
        )
    ));
    var started = System.nanoTime();

    var failure = awaitFailure(authContext.checkAuthenticationStatus());

    assertGaveUpInTime(started);
    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(
        elapsed.toMillis() < ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS + 1_000,
        "took " + elapsed
    );
    assertInstanceOf(TimeoutException.class, failure);
  }

  @Test
  void tokenRefreshShouldCountAStalledExchangeAsAFailedAttempt() throws Exception {
    var authContext = new CCloudOAuthContext();
    awaitResult(authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null));
    stall(WireMock.post(WireMock.urlPathEqualTo("/oauth/token")).atPriority(1));
    var started = System.nanoTime();

    awaitFailure(authContext.refresh(null));

    assertGaveUpInTime(started);
    assertEquals(1, authContext.getFailedTokenRefreshAttempts());
  }

  private void stall(MappingBuilder request) {
    wireMock.register(request.willReturn(WireMock.aResponse().withStatus(200)
        .withFixedDelay(ShortWebClientTimeoutProfile.STALL_MS)
        .withBody(CHECK_JWT_BODY)));
  }

  private static void assertGaveUpInTime(long startedNanos) {
    var elapsed = Duration.ofNanos(System.nanoTime() - startedNanos);
    assertTrue(
        elapsed.toMillis() >= ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS,
        "gave up before the timeout: " + elapsed
    );
    assertTrue(
        elapsed.toMillis() < ShortWebClientTimeoutProfile.GIVE_UP_WITHIN_MS,
        "took " + elapsed
    );
  }

  private static <T> T awaitResult(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private static Throwable awaitFailure(Future<?> future) throws Exception {
    var failure = future
        .map(result -> (Throwable) new AssertionError("expected a failure, got " + result))
        .recover(Future::succeededFuture);
    return awaitResult(failure);
  }
}
