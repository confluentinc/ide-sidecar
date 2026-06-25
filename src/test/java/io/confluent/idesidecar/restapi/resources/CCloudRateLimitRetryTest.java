package io.confluent.idesidecar.restapi.resources;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.queryGraphQLRaw;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying that 429 responses from CCloud APIs are handled with retry, that
 * rate-limit response headers are used for adaptive throttling, and that other error status codes
 * are propagated correctly.
 */
@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class CCloudRateLimitRetryTest extends ConfluentQueryResourceTestBase {

  private static final String CONNECTION_ID = "ccloud-retry-test";
  private static final String ENVIRONMENTS_QUERY = """
      {
        ccloudConnectionById(id: "%s") {
          id
          environments {
            id
          }
        }
      }
      """.formatted(CONNECTION_ID);
  private String bearerToken;

  @Inject
  CCloudApiRateLimiter rateLimiter;

  @BeforeEach
  void setup() {
    super.setup();
    // limiter is @ApplicationScoped: clear leftover state from prior @Test methods so direct
    // assertions on getLatestState() / inflight count aren't polluted across tests
    rateLimiter.reset();

    ccloudTestUtil.createAuthedConnection(
        CONNECTION_ID,
        "CCloud Retry Test",
        ConnectionType.CCLOUD
    );
    bearerToken = ccloudTestUtil.getControlPlaneToken(CONNECTION_ID);

    // always register the org mock (environments query doesn't need it to fail)
    ccloudTestUtil.expectSuccessfulCCloudGet(
        orgListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-organizations.json"
    );
  }

  @AfterEach
  void afterEach() {
    super.afterEach();
  }

  /**
   * Verifies that when the environments endpoint returns 429 on the first attempt and 200 on the
   * second, the GraphQL query succeeds transparently. Both responses include rate-limit headers
   * so the adaptive limiter can update its state.
   */
  @Test
  void shouldRetryOnEnv429AndSucceed() {
    // arrange: first call to env list returns 429 with rate-limit headers,
    // second returns 200 with healthy remaining quota
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .inScenario("env-rate-limit")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("X-RateLimit-Reset", "1")
                    .withBody("{\"error\":{\"message\":\"Rate limit exceeded\"}}")
            )
            .willSetStateTo("retried")
    );
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .inScenario("env-rate-limit")
            .whenScenarioStateIs("retried")
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "9")
                    .withHeader("X-RateLimit-Reset", "60")
                    .withBody(
                        loadResource("ccloud-resources-mock-responses/list-environments.json")
                    )
            )
    );

    // act + assert: query for environments succeeds after retry
    queryGraphQLRaw(ENVIRONMENTS_QUERY)
        .body("data.ccloudConnectionById.environments", notNullValue())
        .body("data.ccloudConnectionById.environments", hasSize(2));
  }

  /**
   * Verifies that successful responses with rate-limit headers are processed correctly and the
   * query succeeds.
   */
  @Test
  void shouldSucceedWithRateLimitHeaders() {
    // arrange: env list returns 200 with rate-limit headers
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("X-RateLimit-Limit", "50")
                    .withHeader("X-RateLimit-Remaining", "45")
                    .withHeader("X-RateLimit-Reset", "60")
                    .withBody(
                        loadResource("ccloud-resources-mock-responses/list-environments.json")
                    )
            )
    );

    // act + assert: query succeeds, rate-limit headers are silently consumed
    queryGraphQLRaw(ENVIRONMENTS_QUERY)
        .body("data.ccloudConnectionById.environments", notNullValue())
        .body("data.ccloudConnectionById.environments", hasSize(2));
  }

  /**
   * Verifies that a non-429 error (e.g. 500) is NOT retried and propagates as an error.
   */
  @Test
  void shouldNotRetryOn500() {
    // arrange: env list always returns 500
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(500)
                    .withBody(
                        "{\"errors\":[{\"status\":\"500\",\"detail\":\"Internal Server Error\"}]}"
                    )
            )
    );

    // act + assert: query returns errors (no retry for 500)
    queryGraphQLRaw(ENVIRONMENTS_QUERY)
        .body("errors", notNullValue())
        .body("errors[0].message", notNullValue());
  }

  /**
   * Verifies that a persistent 429 (all retries exhausted) surfaces as a GraphQL error rather
   * than hanging indefinitely.
   */
  @Test
  void shouldFailAfterMaxRetriesOn429() {
    // arrange: env list always returns 429 with rate-limit headers
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("X-RateLimit-Reset", "1")
                    .withBody("{\"error\":{\"message\":\"Rate limit exceeded\"}}")
            )
    );

    // act + assert: query returns errors after retries exhausted
    queryGraphQLRaw(ENVIRONMENTS_QUERY)
        .body("errors", notNullValue())
        .body("errors[0].message", notNullValue());
  }

  /**
   * Verifies that a persistent 429 is surfaced as a GraphQL DataFetchingException. The specific
   * rate-limit message is logged server-side but SmallRye GraphQL only exposes allowed exception
   * types to clients.
   */
  @Test
  void shouldSurfaceRateLimitAsDataFetchingException() {
    // arrange: always 429 with Retry-After: 30
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "30")
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("X-RateLimit-Reset", "30")
                    .withBody("{\"error\":{\"message\":\"Rate limit exceeded\"}}")
            )
    );

    queryGraphQLRaw(ENVIRONMENTS_QUERY)
        .body("errors", notNullValue())
        .body("errors[0].extensions.classification", is("DataFetchingException"));
  }
}
