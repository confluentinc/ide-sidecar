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

  @AfterEach
  void resetWireMock() {
    wireMock.removeMappings();
  }

@Test
void getDefaultWebClientOptionsIncludesUserAgent() {
  var userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();
  Assertions.assertTrue(userAgent.contains("support@confluent.io) sidecar/"));
}

  @Test
  public void webClientOptionsShouldContainUserAgentOnCreation(
      WireMock wireMock,
      UrlPattern urlPattern,
      int responseStatus,
      String responseJson) {

      wireMock.register(
          WireMock
              .get(urlPattern)
              .willReturn(
                  WireMock
                      .aResponse()
                      .withStatus(responseStatus)
                      .withBody(responseJson)
              )
              .atPriority(100)
      );

}

  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    Assertions.assertSame(firstInstance, secondInstance);
  }
}
