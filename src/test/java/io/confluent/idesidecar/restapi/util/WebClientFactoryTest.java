package io.confluent.idesidecar.restapi.util;


import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import io.confluent.idesidecar.restapi.featureflags.FeatureFlags;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;

@QuarkusTest
@ConnectWireMock
public class WebClientFactoryTest {
  @Inject WebClientFactory webClientFactory;

//  @InjectMock
//  WebClientFactory webClientFactory;

  WireMock wireMock;

//  @BeforeEach
//  void registerWireMockRoutes() {
//    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
//    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(
//        FAKE_AUTHORIZATION_CODE,
//        "Development Org",
//        null
//    );
//  }

  @AfterEach
  void resetWireMock() {
    wireMock.removeMappings();
  }

  //EXAMPLES
//  public static void whenLaunchDarklyReturns(
//      WireMock wireMock,
//      UrlPattern urlPattern,
//      int responseStatus,
//      String responseJson
//  ) {
//    wireMock.register(
//        WireMock
//            .get(urlPattern)
//            .willReturn(
//                WireMock
//                    .aResponse()
//                    .withStatus(responseStatus)
//                    .withBody(responseJson)
//            )
//            .atPriority(100)
//    );
//  }
//  @Test
//  void testConsumePropagatesNon200StatusFromCCloud() {
//    setupSimpleConsumeApi();
//    wireMock.register(
//        WireMock
//            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
//                KAFKA_CLUSTER_ID, "topic_429"))
//            .withHeader(
//                "Authorization",
//                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
//            )
//            .willReturn(
//                WireMock
//                    .aResponse()
//                    .withStatus(429)
//                    .withHeader("Content-Type", "application/json")
//                    .withHeader("x-connection-id", CONNECTION_ID)
//                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
//                    .withBody("Too many requests.")
//            )
//            .atPriority(100));
//    final String path = "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
//        .formatted(KAFKA_CLUSTER_ID, "topic_429");
//    // Now trying to hit the cluster proxy endpoint
//    // should return a 429 Too many requests error.
//    given()
//        .when()
//        .contentType(ContentType.JSON)
//        .header("x-connection-id", CONNECTION_ID)
//        .body("{}")
//        .header("x-cluster-id", KAFKA_CLUSTER_ID)
//        .post(path)
//        .then()
//        .statusCode(429)
//        .contentType(MediaType.APPLICATION_JSON)
//        .body("title", containsString(
//            "Error fetching the messages from ccloud"))
//        .body("title", containsString("Too many requests"));
//  }

@Test
void getDefaultWebClientOptionsIncludesUserAgent() {
  var userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();
  Assertions.assertTrue(userAgent.contains("support@confluent.io) sidecar/"));
}


  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    Assertions.assertSame(firstInstance, secondInstance);
  }
}
