package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.ExceptionUtil.unwrap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryRequestProcessor<T extends ClusterProxyContext> extends
    Processor<T, Future<T>> {

  @Override
  public Future<T> process(T context) {
    var srClientConfig = ClientConfigurator.getSchemaRegistryClientConfig(
        context.getConnectionState(),
        context.getClusterInfo().uri(),
        false,
        Duration.ofSeconds(5)
    );
    var restService = new RestService(context.getClusterInfo().uri());
    restService.configure(srClientConfig);
    var sslFactory = new SslFactory(srClientConfig);
    if (sslFactory.sslContext() != null) {
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
    }
    Log.infof("Creating Schema Registry client with configuration: %s", srClientConfig);
    Map<String, String> headers = Map.of();
    if (context.getProxyRequestHeaders() != null) {
      headers = context
          .getProxyRequestHeaders()
          .entries()
          .stream()
          .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
    }
    var typeReference = new TypeReference<JsonNode>() {
    };

    byte[] requestBody = null;
    if (context.getProxyRequestBody() != null) {
      requestBody = context.getProxyRequestBody().getBytes();
    }

    try {
      var resp = restService.httpRequest(
          context.getRequestUri(),
          context.getProxyRequestMethod().name(),
          requestBody,
          headers,
          typeReference
      );
      if (resp != null) {
        context.setProxyResponseBody(Buffer.buffer(resp.toString()));
      }
      context.setProxyResponseStatusCode(200);
      context.setProxyResponseHeaders(MultiMap.caseInsensitiveMultiMap());
    } catch (RestClientException e) {
      var errorJson = JsonNodeFactory.instance.objectNode();
      errorJson.put("message", e.getMessage());
      errorJson.put("error_code", e.getErrorCode());
      context.setProxyResponseBody(Buffer.buffer(errorJson.toString()));
      context.setProxyResponseStatusCode(e.getStatus());
      context.setProxyResponseHeaders(MultiMap.caseInsensitiveMultiMap());
    } catch (Exception e) {
      throw new ProcessorFailedException(
          context.fail(500, unwrap(e).getMessage())
      );
    }

    return Future.succeededFuture(context);
  }
}
