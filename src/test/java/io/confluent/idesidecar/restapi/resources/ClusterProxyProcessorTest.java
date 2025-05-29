package io.confluent.idesidecar.restapi.resources;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.dockerjava.api.model.ClusterInfo;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ClusterStrategy;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class ClusterProxyProcessorTest {

  ClusterProxyProcessor processor;
  WebClientFactory webClientFactory;
  Processor<ClusterProxyContext, Future<ClusterProxyContext>> nextProcessor;

  @BeforeEach
  void setUp() {
    webClientFactory = new WebClientFactory();
    nextProcessor = new Processor<>() {
      @Override
      public Future<ClusterProxyContext> process(ClusterProxyContext context) {
        return Future.succeededFuture(context);
      }
    };
    processor = new ClusterProxyProcessor();
    processor.setNext(nextProcessor);
    // Use reflection to set package-private fields for testing
    try {
      var webClientFactoryField = ClusterProxyProcessor.class.getDeclaredField("webClientFactory");
      webClientFactoryField.setAccessible(true);
      webClientFactoryField.set(processor, webClientFactory);

      var sidecarHostField = ClusterProxyProcessor.class.getDeclaredField("sidecarHost");
      sidecarHostField.setAccessible(true);
      sidecarHostField.set(processor, "http://sidecar:8080");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ClusterProxyContext createContext(
      ClusterType type,
      ClusterStrategy strategy,
      String clusterUri,
      CCloudConnectionState state,
      Buffer body
  ) {
    return new ClusterProxyContext(
        "/foo",
        MultiMap.caseInsensitiveMultiMap(),
        io.vertx.core.http.HttpMethod.GET,
        body,
        Map.of(),
        "conn-id",
        "id1",
        ClusterType.SCHEMA_REGISTRY
    );
  }

  @Test
  void testKafkaCluster_setsProxyRequestFields() {
    // Mock CCloudConnectionState
    CCloudConnectionState state = mock(CCloudConnectionState.class);

    var strategy = new ClusterStrategy() {
      @Override
      public String constructProxyUri(String requestUri, String clusterUri) {
        return clusterUri + requestUri;
      }
      @Override
      public MultiMap constructProxyHeaders(ClusterProxyContext ctx) {
        return MultiMap.caseInsensitiveMultiMap();
      }
      @Override
      public String processProxyResponse(String body, String clusterUri, String sidecarHost) {
        return body;
      }
    };
    var context = createContext(ClusterType.KAFKA, strategy, "http://cluster:9092", state, Buffer.buffer("body"));

    var result = processor.process(context).result();

    assertEquals("http://cluster:9092/foo", result.getProxyRequestAbsoluteUrl());
    assertEquals(io.vertx.core.http.HttpMethod.GET, result.getProxyRequestMethod());
    assertEquals("body", result.getProxyRequestBody().toString());
  }
  @Test
  void testSchemaRegistryCluster_setsTLSOptions_withJks() {
    var truststore = "/truststore.jks";
    var keystore = "/keystore.jks";
    var tlsConfig = new TLSConfig(
        truststore, new Password("trustpass".toCharArray()),
        keystore, new Password("keypass".toCharArray()), new Password("aliaspass".toCharArray())
    );
    // Mock CCloudConnectionState and override getSchemaRegistryTLSConfig
    CCloudConnectionState state = mock(CCloudConnectionState.class);
    when(state.getSchemaRegistryTLSConfig()).thenReturn(Optional.of(tlsConfig));

    var strategy = new ClusterStrategy() {
      @Override
      public String constructProxyUri(String requestUri, String clusterUri) {
        return clusterUri + requestUri;
      }
      @Override
      public MultiMap constructProxyHeaders(ClusterProxyContext ctx) {
        return MultiMap.caseInsensitiveMultiMap();
      }
      @Override
      public String processProxyResponse(String body, String clusterUri, String sidecarHost) {
        return body;
      }
    };
    var context = createContext(ClusterType.SCHEMA_REGISTRY, strategy, "http://cluster:9092", state, Buffer.buffer("body"));

    var result = processor.process(context).result();

    var options = result.getWebClientOptions();
    assertNotNull(options);
    assertEquals("/truststore.jks", ((JksOptions) options.getTrustStoreOptions()).getPath());
    assertEquals("/keystore.jks", ((JksOptions) options.getKeyStoreOptions()).getPath());
    assertEquals("keypass", ((JksOptions) options.getKeyStoreOptions()).getPassword());
    assertEquals("aliaspass", ((JksOptions) options.getKeyStoreOptions()).getAliasPassword());
  }

  @Test
  void testSchemaRegistryCluster_setsTLSOptions_withOnlyTruststore() {
    var truststore = "/truststore.jks";
    var tlsConfig = new TLSConfig(
        truststore, new Password("trustpass".toCharArray()),
        null, null, null
    );
    // Mock CCloudConnectionState and override getSchemaRegistryTLSConfig
    CCloudConnectionState state = mock(CCloudConnectionState.class);
    when(state.getSchemaRegistryTLSConfig()).thenReturn(Optional.of(tlsConfig));

    var strategy = new ClusterStrategy() {
      @Override
      public String constructProxyUri(String requestUri, String clusterUri) {
        return clusterUri + requestUri;
      }
      @Override
      public MultiMap constructProxyHeaders(ClusterProxyContext ctx) {
        return MultiMap.caseInsensitiveMultiMap();
      }
      @Override
      public String processProxyResponse(String body, String clusterUri, String sidecarHost) {
        return body;
      }
    };
    var context = createContext(ClusterType.SCHEMA_REGISTRY, strategy, "http://cluster:9092", state, Buffer.buffer("body"));

    var result = processor.process(context).result();

    var options = result.getWebClientOptions();
    assertNotNull(options);
    assertEquals("/truststore.jks", ((JksOptions) options.getTrustStoreOptions()).getPath());
    assertNull(options.getKeyStoreOptions());
  }

  @Test
  void testSchemaRegistryCluster_setsTLSOptions_withOnlyKeystore() {
    var keystore = "/keystore.jks";
    var tlsConfig = new TLSConfig(
        null, null,
        keystore, new Password("keypass".toCharArray()), new Password("aliaspass".toCharArray())
    );
    // Mock CCloudConnectionState and override getSchemaRegistryTLSConfig
    CCloudConnectionState state = mock(CCloudConnectionState.class);
    when(state.getSchemaRegistryTLSConfig()).thenReturn(Optional.of(tlsConfig));

    var strategy = new ClusterStrategy() {
      @Override
      public String constructProxyUri(String requestUri, String clusterUri) {
        return clusterUri + requestUri;
      }
      @Override
      public MultiMap constructProxyHeaders(ClusterProxyContext ctx) {
        return MultiMap.caseInsensitiveMultiMap();
      }
      @Override
      public String processProxyResponse(String body, String clusterUri, String sidecarHost) {
        return body;
      }
    };
    var context = createContext(ClusterType.SCHEMA_REGISTRY, strategy, "http://cluster:9092", state, Buffer.buffer("body"));

    var result = processor.process(context).result();

    var options = result.getWebClientOptions();
    assertNotNull(options);
    assertNull(options.getTrustStoreOptions());
    assertEquals("/keystore.jks", ((JksOptions) options.getKeyStoreOptions()).getPath());
  }

  @Test
  void testSchemaRegistryCluster_tlsConfigMissing_noNPE() {
    // Mock CCloudConnectionState and override getSchemaRegistryTLSConfig
    CCloudConnectionState state = mock(CCloudConnectionState.class);
    when(state.getSchemaRegistryTLSConfig()).thenReturn(Optional.empty());

    var strategy = new ClusterStrategy() {
      @Override
      public String constructProxyUri(String requestUri, String clusterUri) {
        return clusterUri + requestUri;
      }
      @Override
      public MultiMap constructProxyHeaders(ClusterProxyContext ctx) {
        return MultiMap.caseInsensitiveMultiMap();
      }
      @Override
      public String processProxyResponse(String body, String clusterUri, String sidecarHost) {
        return body;
      }
    };
    var context = createContext(ClusterType.SCHEMA_REGISTRY, strategy, "http://cluster:9092", state, Buffer.buffer("body"));

    assertDoesNotThrow(() -> processor.process(context).result());
    assertNull(context.getWebClientOptions());
  }
}