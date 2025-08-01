package io.confluent.idesidecar.websocket.proxy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import jakarta.websocket.Session;

@QuarkusTest
class ProxyContextTest {

  @InjectMock
  FlinkPrivateEndpointUtil flinkPrivateEndpointUtil;

  static final String TEST_CONNECTION_ID = "conn1";
  static final String TEST_ENVIRONMENT_ID = "env-123";
  static final String TEST_REGION = "us-west-2";
  static final String TEST_PROVIDER = "aws";
  static final String TEST_ORGANIZATION_ID = "org-456";
  static final ProxyContext PROXY_CONTEXT_AWS_US_WEST = new ProxyContext(
      TEST_CONNECTION_ID,
      TEST_REGION,
      TEST_PROVIDER,
      TEST_ENVIRONMENT_ID,
      TEST_ORGANIZATION_ID,
      null
  );

  @Test
  void testFromSession() {
    // Given
    Session session = mock(Session.class);
    Map<String, List<String>> paramMap = Map.of(
        "connectionId", List.of(TEST_CONNECTION_ID),
        "region", List.of(TEST_REGION),
        "provider", List.of(TEST_PROVIDER),
        "environmentId", List.of(TEST_ENVIRONMENT_ID),
        "organizationId", List.of(TEST_ORGANIZATION_ID)
    );
    when(session.getRequestParameterMap()).thenReturn(paramMap);

    ProxyContext context = ProxyContext.from(session);

    assertEquals(TEST_CONNECTION_ID, context.connectionId());
    assertEquals(TEST_REGION, context.region());
    assertEquals(TEST_PROVIDER, context.provider());
    assertEquals(TEST_ENVIRONMENT_ID, context.environmentId());
    assertEquals(TEST_ORGANIZATION_ID, context.organizationId());
    assertNull(context.connection());
  }

  @Test
  void testTransformPrivateEndpointToLanguageServiceUrl() {
    // Given: Matching private endpoint exists
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    String privateEndpoint = "https://flink.us-west-2.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
        .thenReturn(List.of(privateEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        privateEndpoint, TEST_REGION, TEST_PROVIDER))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should transform to language service URL
    assertEquals("wss://flinkpls.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testTransformCCNPrivateEndpointToLanguageServiceUrl() {
    // Given: CCN private endpoint with domain ID
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    String ccnEndpoint = "https://flink.domid123.us-west-2.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
        .thenReturn(List.of(ccnEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        ccnEndpoint, TEST_REGION, TEST_PROVIDER))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should handle CCN format correctly
    assertEquals("wss://flinkpls.domid123.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testFallbackToPublicUrlWhenNoPrivateEndpoints() {
    // Given: No private endpoints configured
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
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
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    String nonMatchingEndpoint = "https://flink.eu-central-1.aws.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
        .thenReturn(List.of(nonMatchingEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        nonMatchingEndpoint, TEST_REGION, TEST_PROVIDER))
        .thenReturn(false);

    String url = context.getConnectUrl();

    // Then: Should fallback to public URL
    assertTrue(url.contains("fls-mock"));
    assertTrue(url.contains("127.0.0.1"));
    assertFalse(url.contains(".private."));
  }

  @Test
  void testSelectsCorrectEndpointFromMultiple() {
    // Given: Multiple private endpoints, only one matches
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    String matchingEndpoint = "https://flink.us-west-2.aws.private.confluent.cloud";
    String wrongRegion = "https://flink.eu-central-1.aws.private.confluent.cloud";
    String wrongProvider = "https://flink.us-west-2.gcp.private.confluent.cloud";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
        .thenReturn(List.of(wrongRegion, matchingEndpoint, wrongProvider));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        matchingEndpoint, TEST_REGION, TEST_PROVIDER))
        .thenReturn(true);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        wrongRegion, TEST_REGION, TEST_PROVIDER))
        .thenReturn(false);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        wrongProvider, TEST_REGION, TEST_PROVIDER))
        .thenReturn(false);

    String url = context.getConnectUrl();

    // Then: Should select the matching endpoint
    assertEquals("wss://flinkpls.us-west-2.aws.private.confluent.cloud/lsp", url);
  }

  @Test
  void testHandlesInvalidPrivateEndpoint() {
    // Given: Invalid private endpoint URL
    ProxyContext context = PROXY_CONTEXT_AWS_US_WEST;
    String invalidEndpoint = "not-a-valid-url";

    when(flinkPrivateEndpointUtil.getPrivateEndpoints(TEST_ENVIRONMENT_ID))
        .thenReturn(List.of(invalidEndpoint));
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        invalidEndpoint, TEST_REGION, TEST_PROVIDER))
        .thenReturn(true);

    String url = context.getConnectUrl();

    // Then: Should fallback to public URL
    assertTrue(url.contains("fls-mock"));
    assertTrue(url.contains("127.0.0.1"));
    assertFalse(url.contains(".private."));
  }
}
