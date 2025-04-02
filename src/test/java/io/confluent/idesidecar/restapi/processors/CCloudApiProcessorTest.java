package io.confluent.idesidecar.restapi.processors;

import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.proxy.CCloudApiProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import jakarta.inject.Inject;

@QuarkusTest
@ConnectWireMock
class CCloudApiProcessorTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  CCloudApiProcessor cCloudApiProcessor;

  private static final String CONNECTION_ID = "fake-connection-id";
  private static final int wireMockPort = 8080;
  CCloudTestUtil ccloudTestUtil;
  private WireMock wireMock;

  @BeforeEach
  void setUp() {
    wireMock = new WireMock(wireMockPort);
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
  }

  @Test
  void testProcessWithInvalidFormat() {
    // Given
    var requestHeaders = MultiMap.caseInsensitiveMultiMap();
    var proxyContext = new ProxyContext(
        "/artifacts",
        requestHeaders,
        HttpMethod.GET,
        Buffer.buffer(),
        Map.of(),
        CONNECTION_ID
    );
    var connectionState = proxyContext.getConnectionState();
    proxyContext.setConnectionState(connectionState);

    // When
    Future<ProxyContext> result = cCloudApiProcessor.process(proxyContext);

    // Then
    assertTrue(result.failed());
    assertEquals("400", ((ProcessorFailedException) result.cause()).getFailure().status());
  }
}
