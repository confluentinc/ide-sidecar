package io.confluent.idesidecar.restapi.util;


import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
public class WebClientFactoryTest {
  @Inject WebClientFactory webClientFactory;

  WireMock wireMock;
  WireMockServer wireMockServer;

  @BeforeEach
  public void setup() {
    wireMockServer = new WireMockServer(options().dynamicPort());
    wireMockServer.start();
    WireMock.configureFor("localhost", wireMockServer.port());
  }
  @AfterEach
  void resetWireMock() {
    wireMockServer.stop();
    wireMock.removeMappings();
  }

  @Test
  void getDefaultWebClientOptionsIncludesUserAgent() {
    var userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();
    Assertions.assertTrue(userAgent.contains("support@confluent.io) sidecar/"));
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

    // Create a WebClient instance with the correct port
    WebClient webClient = WebClient.create(
        Vertx.vertx(), new WebClientOptions().setDefaultHost("localhost").setUserAgent("Confluent-for-VSCode/").setDefaultPort(wireMockServer.port()));

    // Make a request using the WebClient
    webClient.get("/some-endpoint")
             .send()
             .onSuccess(response -> {
               // Verify the response
               assertEquals(200, response.statusCode());
             })
             .onFailure(Throwable::printStackTrace)
             .toCompletionStage()
             .toCompletableFuture()
             .join(); // Ensure the request completes before the test ends

    // Verify that the request contains the User-Agent header
    wireMockServer.verify(
        WireMock.getRequestedFor(urlPattern)
                .withHeader("User-Agent", WireMock.matching(".*Confluent-for-VSCode/*"))
    );
  }

  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    Assertions.assertSame(firstInstance, secondInstance);
  }
}
