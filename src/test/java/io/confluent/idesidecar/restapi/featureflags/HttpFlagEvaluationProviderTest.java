package io.confluent.idesidecar.restapi.featureflags;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
class HttpFlagEvaluationProviderTest extends BaseFeatureFlagsTest {

  HttpFlagEvaluationProvider provider;

  WireMock wireMock;

  @Inject
  WebClientFactory webClientFactory;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  @ConfigProperty(name = "ide-sidecar.feature-flags.ide-project.eval-uri")
  String httpUrl;

  @BeforeEach
  void setup() {
    provider = new HttpFlagEvaluationProvider(
        PROJECT_NAME,
        CLIENT_ID,
        "http://localhost:%d/api/eval".formatted(wireMockPort),
        MAPPER
    );
  }

  @AfterEach
  void cleanUp() {
    wireMock.removeMappings();
  }

  @Test
  public void shouldGetEvaluationsWithMultipleFlags() {
    // When evaluating the flags
    whenLaunchDarklyReturns(
        wireMock,
        urlPathEqualTo("/api/eval"),
        200,
        loadResource(LAUNCH_DARKLY_RESPONSE_RESOURCE_PATH)
    );
    var evaluations = assertProviderRefresh(provider, CONTEXT, webClientFactory);

    // Then the resulting evaluations should match the expected results
    var expected = loadEvaluationsFromResourceFile(
        LAUNCH_DARKLY_RESPONSE_RESOURCE_PATH,
        PROJECT_NAME
    );
    assertEquals(7, evaluations.size());
    assertEquals(expected, evaluations);
  }

  @Test
  public void shouldGetEvaluationsWithNoFlags() {
    // When evaluating the flags
    whenLaunchDarklyReturns(
        wireMock,
        urlPathEqualTo("/api/eval"),
        200,
        loadResource(LAUNCH_DARKLY_EMPTY_RESOURCE_PATH)
    );
    var evaluations = assertProviderRefresh(provider, CONTEXT, webClientFactory);

    // Then the resulting evaluations should match the expected results
    assertTrue(evaluations.isEmpty());
  }

  @Test
  public void shouldFailToGetEvaluations() {
    // When evaluating the flags
    whenLaunchDarklyReturns(
        wireMock,
        urlPathEqualTo("/api/eval"),
        400,
        loadResource(LAUNCH_DARKLY_ERROR_RESOURCE_PATH)
    );
    var evaluations = assertProviderRefresh(provider, CONTEXT, webClientFactory);

    // Then the resulting evaluations should match the expected results
    assertNull(evaluations);
  }

  @Test
  public void shouldFailToGetEvaluationsWithUnexpectedResponse() {
    // When evaluating the flags
    whenLaunchDarklyReturns(
        wireMock,
        urlPathEqualTo("/api/eval"),
        400,
        "This is not a valid JSON response"
    );
    var evaluations = assertProviderRefresh(provider, CONTEXT, webClientFactory);

    // Then the resulting evaluations should match the expected results
    assertNull(evaluations);
  }
}
