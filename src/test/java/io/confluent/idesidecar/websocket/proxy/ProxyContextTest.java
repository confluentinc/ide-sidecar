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
        "us-west-2",  // Changed to match test private endpoints
        "aws",
        "env-123",
        "org-456",
        null
    );
  }

  private ProxyContext createContextWithDifferentRegion() {
    return new ProxyContext(
        "conn1",
        "eu-central-1",
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
  void testTransformPrivateEndpointToLanguageServiceUrl() {
    // Given: Matching private endpoint exists
    ProxyContext context = createContext(); // us-west-2, aws
    String privateEndpoint = "https://flink.us-west-2.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of(privateEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        privateEndpoint, "us-west-2", "aws"))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should transform to language service URL
    assertEquals("wss://flinkpls.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testTransformCCNPrivateEndpointToLanguageServiceUrl() {
    // Given: CCN private endpoint with domain ID
    ProxyContext context = createContext(); // us-west-2, aws
    String ccnEndpoint = "https://flink.domid123.us-west-2.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of(ccnEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        ccnEndpoint, "us-west-2", "aws"))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should handle CCN format correctly
    assertEquals("wss://flinkpls.domid123.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testFallbackToPublicUrlWhenNoPrivateEndpoints() {
    // Given: No private endpoints configured
    ProxyContext context = createContext();
    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of());

    String url = context.getConnectUrl();

    // Then: Should use public URL pattern from config (test pattern)
    assertTrue(url.contains("fls-mock"));
    assertTrue(url.contains("127.0.0.1"));
    assertFalse(url.contains(".private."));
  }

  @Test
  void testFallbackToPublicUrlWhenNoMatchingPrivateEndpoints() {
    // Given: Private endpoints exist but don't match region/provider
    ProxyContext context = createContext(); // us-west-2, aws
    String nonMatchingEndpoint = "https://flink.eu-central-1.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of(nonMatchingEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        nonMatchingEndpoint, "us-west-2", "aws"))
        .thenReturn(false);

    String url = context.getConnectUrl();

    // Then: Should fallback to public URL (test pattern)
    assertTrue(url.contains("fls-mock"));
    assertTrue(url.contains("127.0.0.1"));
    assertFalse(url.contains(".private."));
  }

  @Test
  void testSelectsCorrectEndpointFromMultiple() {
    // Given: Multiple private endpoints, only one matches
    ProxyContext context = createContext(); // us-west-2, aws
    String matchingEndpoint = "https://flink.us-west-2.aws.private.confluent.cloud";
    String wrongRegion = "https://flink.eu-central-1.aws.private.confluent.cloud";
    String wrongProvider = "https://flink.us-west-2.gcp.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of(wrongRegion, matchingEndpoint, wrongProvider));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        matchingEndpoint, "us-west-2", "aws"))
        .thenReturn(true);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        wrongRegion, "us-west-2", "aws"))
        .thenReturn(false);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        wrongProvider, "us-west-2", "aws"))
        .thenReturn(false);

    String url = context.getConnectUrl();

    // Then: Should select the matching endpoint
    assertEquals("wss://flinkpls.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testHandlesInvalidPrivateEndpoint() {
    // Given: Invalid private endpoint URL
    ProxyContext context = createContext();
    String invalidEndpoint = "not-a-valid-url";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints("env-123"))
        .thenReturn(List.of(invalidEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        invalidEndpoint, "us-west-2", "aws"))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should fallback to public URL
    assertTrue(url.contains("fls-mock"));
    assertTrue(url.contains("127.0.0.1"));
    assertFalse(url.contains(".private."));
  }
}
