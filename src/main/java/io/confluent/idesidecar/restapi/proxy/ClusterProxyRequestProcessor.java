package io.confluent.idesidecar.restapi.proxy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic processor that ships the request to the target server and updates the context with the
 * response.
 *
 */
@ApplicationScoped
public class ClusterProxyRequestProcessor extends
    Processor<ClusterProxyContext, Future<ClusterProxyContext>> {
  @Inject
  SchemaRegistryClients clients;

  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  ProxyHttpClient<ClusterProxyContext> proxyHttpClient;

  @Inject
  public ClusterProxyRequestProcessor(WebClientFactory webClientFactory, Vertx vertx) {
    proxyHttpClient = new ProxyHttpClient<>(webClientFactory, vertx);
  }

  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    var clusterOp = switch (context.getClusterType()) {
      // If Kafka, send the request to the Kafka REST server
      // using a generic HTTP client
      case KAFKA -> proxyHttpClient.send(context);
      // If Schema Registry, use the cached SchemaRegistryClient instance
      // to send the request to the Schema Registry server. Constructing the full
      // HTTP request ourselves is error-prone and unnecessary.
      case SCHEMA_REGISTRY -> processSchemaRegistry(context);
    };

    return clusterOp.compose(
        processedContext -> next().process(processedContext)
    );
  }

  public Future<ClusterProxyContext> processSchemaRegistry(ClusterProxyContext context) {
    var client = clients.getClient(context.getConnectionId(), context.getClusterId());
    try {
      var resp = client.httpRequest(
          context.getRequestUri(),
          context.getProxyRequestMethod().toString(),
          Optional
              .ofNullable(context.getProxyRequestBody())
              .map(Buffer::getBytes)
              .orElse(null),
          sanitizeHeaders(context.getProxyRequestHeaders()),
          new TypeReference<JsonNode>() {
          }
      );

      // If we get here, all went well
      context.setProxyResponseBody(Buffer.buffer(resp.toString()));

      context.setProxyResponseStatusCode(Response.Status.OK.getStatusCode());
      // See: https://docs.confluent.io/platform/current/schema-registry/develop/api.html#content-types
      // "application/json" is permitted as a "less specific content type" for all Schema Registry
      // API responses.
      context.setProxyResponseHeaders(
          HttpHeaders.headers().add("Content-Type", "application/json")
      );
    } catch (IOException e) {
      context.failf(
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          "Failed to process Schema Registry request: %s", e.getMessage()
      );
    } catch (RestClientException e) {
      context.setProxyResponseStatusCode(
          e.getStatus()
      );

      context.setProxyResponseBody(
          Buffer.buffer(
              Json.createObjectBuilder()
                  .add("error_code", e.getErrorCode())
                  .add("message", e.getMessage())
                  .build()
                  .toString()
          )
      );

      context.setProxyResponseHeaders(
          HttpHeaders.headers().add("Content-Type", "application/json")
      );
    }

    return Future.succeededFuture(context);
  }

  public Map<String, String> sanitizeHeaders(MultiMap requestHeaders) {
    var headers = new HashMap<String, String>();
    requestHeaders.forEach(
        entry -> headers.put(entry.getKey(), entry.getValue())
    );
    httpHeaderExclusions.forEach(headers::remove);
    return headers;
  }
}
