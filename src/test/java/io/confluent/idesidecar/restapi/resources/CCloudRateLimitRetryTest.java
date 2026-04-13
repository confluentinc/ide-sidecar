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
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying that 429 responses from CCloud APIs are handled with retry and that
 * other error status codes are propagated correctly.
 */
@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class CCloudRateLimitRetryTest extends ConfluentQueryResourceTestBase {

  private static final String CONNECTION_ID = "ccloud-retry-test";
  private String bearerToken;

  @BeforeEach
  void setup() {
    super.setup();
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
   * second, the GraphQL query succeeds transparently.
   */
  @Test
  void shouldRetryOnEnv429AndSucceed() {
    // arrange: first call to env list returns 429, second returns 200
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
                    .withBody(
                        loadResource("ccloud-resources-mock-responses/list-environments.json")
                    )
            )
    );

    // act + assert: query for environments succeeds after retry
    var query = """
        {
          ccloudConnectionById(id: "%s") {
            id
            environments {
              id
            }
          }
        }
        """.formatted(CONNECTION_ID);
    queryGraphQLRaw(query)
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
    var query = """
        {
          ccloudConnectionById(id: "%s") {
            id
            environments {
              id
            }
          }
        }
        """.formatted(CONNECTION_ID);
    queryGraphQLRaw(query)
        .body("errors", notNullValue())
        .body("errors[0].message", notNullValue());
  }

  /**
   * Verifies that a persistent 429 (all retries exhausted) surfaces as a GraphQL error rather
   * than hanging indefinitely.
   */
  @Test
  void shouldFailAfterMaxRetriesOn429() {
    // arrange: env list always returns 429
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(envListUri))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withBody("{\"error\":{\"message\":\"Rate limit exceeded\"}}")
            )
    );

    // act + assert: query returns errors after retries exhausted
    var query = """
        {
          ccloudConnectionById(id: "%s") {
            id
            environments {
              id
            }
          }
        }
        """.formatted(CONNECTION_ID);
    queryGraphQLRaw(query)
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
                    .withBody("{\"error\":{\"message\":\"Rate limit exceeded\"}}")
            )
    );

    var query = """
        {
          ccloudConnectionById(id: "%s") {
            id
            environments {
              id
            }
          }
        }
        """.formatted(CONNECTION_ID);
    queryGraphQLRaw(query)
        .body("errors", notNullValue())
        .body("errors[0].extensions.classification", is("DataFetchingException"));
  }
}
