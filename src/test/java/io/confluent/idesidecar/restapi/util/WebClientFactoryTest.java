package io.confluent.idesidecar.restapi.util;


import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectSpy;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
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

  @AfterEach
  void resetWireMock() {
    wireMockServer.stop();
    wireMock.removeMappings();
  }

  @Test
  void getDefaultWebClientOptionsIncludesUserAgent() {
    var userAgent = webClientFactory.getDefaultWebClientOptions().getUserAgent();
    assertTrue(userAgent.contains("support@confluent.io) sidecar/"));
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

    String userAgent = sidecarInfo.getUserAgent();

    // Create a WebClient instance with the correct port
    WebClient webClient = WebClient.create(
        Vertx.vertx(),
        new WebClientOptions()
            .setDefaultHost("localhost")
            .setUserAgent(
                "Confluent-for-VSCode/v%s (https://confluent.io; support@confluent.io) sidecar/v%s (%s/%s)".formatted(
                    sidecarInfo.vsCode(),
                    sidecarInfo.version(),
                    sidecarInfo.osType(),
                    sidecarInfo.osArch()
                )
            )
            .setDefaultPort(wireMockServer.port())
    );
    // Verify that the request contains the User-Agent header
    wireMockServer.verify(
        WireMock.getRequestedFor(urlPattern)
                .withHeader("User-Agent",
                    WireMock
                        .equalTo((userAgent))
                )
    );
  }

  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    assertSame(firstInstance, secondInstance);
  }
}
