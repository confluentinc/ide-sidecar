package io.confluent.idesidecar.restapi.util;


import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.testutil.FakeSideCarProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.ext.web.client.WebClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FakeSideCarProfile.class)
public class WebClientFactoryTest {
  @Inject WebClientFactory webClientFactory;
  @Inject SidecarInfo sidecarInfo;

  WireMock wireMock;
  WireMockServer wireMockServer;

  @BeforeEach
  public void setup() {
    wireMockServer = new WireMockServer(options().dynamicPort());
    wireMockServer.start();
    WireMock.configureFor("localhost", wireMockServer.port());
  }

  @Test
  void getDefaultWebClientOptionsIncludesUserAgent() {
    var userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();
    assertTrue(userAgent.contains("Confluent-for-VSCode/"));
  }


  @Test
  public void webClientOptionsShouldContainUserAgentOnCreation() {
    UrlPattern urlPattern = urlEqualTo("/some-endpoint");
    // Register the WireMock stub
    wireMockServer.stubFor(
        WireMock.get(urlPattern)
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody("{\"message\":\"success\"}")
                )
    );

    String userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();

    WebClient webClient = webClientFactory.getWebClient();
    webClient
        .getAbs(wireMockServer.baseUrl() + "/some-endpoint")
        .send();


    webClient.get("/some-endpoint").send(ar -> {
      if (ar.succeeded()) {
        wireMockServer.verify(
            WireMock.getRequestedFor(urlPattern)
                    .withHeader("User-Agent",
                        WireMock
                            .equalTo(userAgent)
                    )
        );
      } else {
        Assertions.fail("Request failed");
      }
    });
  }

  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    assertSame(firstInstance, secondInstance);
  }
}
