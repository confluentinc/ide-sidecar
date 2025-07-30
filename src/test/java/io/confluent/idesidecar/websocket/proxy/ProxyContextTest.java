package io.confluent.idesidecar.websocket.proxy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import java.util.List;

@QuarkusTest
class ProxyContextTest {

  @InjectMock
  FlinkPrivateEndpointUtil flinkPrivateEndpointUtil;

  private ProxyContext createContext() {
    return new ProxyContext(
        "conn1",
        "us-east-1",
        "aws",
        "env-123",
        "org-456",
        null
    );
  }

  @Test
  void testFromSession() {
    // Given
    jakarta.websocket.Session session = mock(jakarta.websocket.Session.class);
    java.util.Map<String, java.util.List<String>> paramMap = java.util.Map.of(
        "connectionId", List.of("test-conn"),
        "region", List.of("eu-west-1"),
        "provider", List.of("gcp"),
        "environmentId", List.of("test-env"),
        "organizationId", List.of("test-org")
    );
    when(session.getRequestParameterMap()).thenReturn(paramMap);

    ProxyContext context = ProxyContext.from(session);

    assertEquals("test-conn", context.connectionId());
    assertEquals("eu-west-1", context.region());
    assertEquals("gcp", context.provider());
    assertEquals("test-env", context.environmentId());
    assertEquals("test-org", context.organizationId());
    assertNull(context.connection());
  }

  @Test
  void testUsePrivateUrlPattern() {
    // Given
    ProxyContext context = createContext();
    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of("private-endpoint"));

    String url = context.getConnectUrl();

    // Then - test that private pattern is used
    assertTrue(url.contains("?private=true"));
    assertTrue(url.contains("127.0.0.1"));
  }

  @Test
  void testUsePublicUrlPattern() {
    // Given
    ProxyContext context = createContext();
    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of());

    String url = context.getConnectUrl();

    // Then - test that public pattern is used
    assertTrue(url.contains("fls-mock"));
    assertFalse(url.contains("private=true"));
  }

  @Test
  void testHasPrivateEndpoints() {
    // Given
    ProxyContext context = createContext();
    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of("endpoint1", "endpoint2"));

    String url = context.getConnectUrl();

    assertTrue(url.contains("?private=true"));
  }

  @Test
  void testNoPrivateEndpoints() {
    // Given
    ProxyContext context = createContext();
    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of());

    String url = context.getConnectUrl();

    assertTrue(url.contains("fls-mock"));
    assertFalse(url.contains("?private=true"));
  }
}
