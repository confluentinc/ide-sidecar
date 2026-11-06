package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.testutil.ShortWebClientTimeoutProfile;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.MultiMap;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Fetches through {@link ConfluentRestClient} give up on a server that stops answering.
 */
@QuarkusTest
@ConnectWireMock
@TestProfile(ShortWebClientTimeoutProfile.class)
class ConfluentRestClientTimeoutTest {

  @Inject
  WebClientFactory webClientFactory;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;

  @AfterEach
  void tearDown() {
    wireMock.removeMappings();
  }

  @Test
  void getItemShouldFailWhenTheServerStalls() {
    wireMock.register(WireMock.get("/stalled-item").willReturn(
        WireMock.aResponse()
            .withStatus(200)
            .withFixedDelay(ShortWebClientTimeoutProfile.STALL_MS)
            .withBody("{}")
    ));
    var client = new ConfluentRestClient() {
    };
    client.webClientFactory = webClientFactory;
    var url = "http://localhost:%d/stalled-item".formatted(wireMockPort);
    var started = System.nanoTime();

    var thrown = assertThrows(
        ExecutionException.class,
        () -> client
            .getItem(MultiMap.caseInsensitiveMultiMap(), url, (itemUrl, json) -> json)
            .subscribeAsCompletionStage()
            .get(10, TimeUnit.SECONDS)
    );

    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(
        elapsed.toMillis() >= ShortWebClientTimeoutProfile.REQUEST_TIMEOUT_MS,
        "gave up before the timeout: " + elapsed
    );
    assertTrue(
        elapsed.toMillis() < ShortWebClientTimeoutProfile.GIVE_UP_WITHIN_MS,
        "took " + elapsed
    );
    assertInstanceOf(TimeoutException.class, thrown.getCause());
  }
}
