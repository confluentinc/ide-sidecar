package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.SanitizeHeadersUtil.sanitizeHeaders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.clients.RetryAfterException;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.clients.SidecarRestService;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.RateLimitState;
import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Generic processor that ships the request to the target server and updates the context with the
 * response.
 */
@ApplicationScoped
public class ClusterProxyRequestProcessor extends
    Processor<ClusterProxyContext, Future<ClusterProxyContext>> {

  /**
   * How long one proxied Schema Registry request may spend waiting out {@code Retry-After}, kept
   * under the extension's own timeout for sidecar calls so the extension sees our error.
   */
  static final Duration SCHEMA_REGISTRY_DEADLINE = Duration.ofSeconds(20);

  @Inject
  SchemaRegistryClients clients;

  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  private final ProxyHttpClient<ClusterProxyContext> proxyHttpClient;

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
      case SCHEMA_REGISTRY -> switch (context.getConnectionState().getType()) {
        // For CCloud connections, use the ProxyHttpClient so that we get access to
        // user-provided HTTP settings, like custom SSL certs
        case CCLOUD -> proxyHttpClient.send(context);
        // For all other connections, use the REST service of the cached SR client
        case DIRECT, LOCAL -> processSchemaRegistry(context);
      };
    };

    return clusterOp.compose(
        processedContext -> next().process(processedContext)
    );
  }

  private Future<ClusterProxyContext> processSchemaRegistry(ClusterProxyContext context) {
    var client = clients.getClient(context.getConnectionId(), context.getClusterId());
    var deadline = Instant.now().plus(SCHEMA_REGISTRY_DEADLINE);
    try {
      var resp = SidecarRestService.withDeadline(
          deadline,
          () -> client.httpRequest(
              context.getRequestUri(),
              context.getProxyRequestMethod().toString(),
              Optional
                  .ofNullable(context.getProxyRequestBody())
                  .map(Buffer::getBytes)
                  .orElse(null),
              sanitizeHeaders(context.getProxyRequestHeaders(), httpHeaderExclusions),
              new TypeReference<JsonNode>() {
              }
          )
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
      Log.errorf(
          e, "Schema Registry request %s for connection %s failed",
          context.getRequestUri(), context.getConnectionId()
      );
      return Future.failedFuture(new ProcessorFailedException(toFailure(context, e, deadline)));
    } catch (RestClientException e) {
      if (e instanceof RetryAfterException || e.getStatus() >= 500) {
        Log.errorf(
            e, "Schema Registry request %s for connection %s failed with %d",
            context.getRequestUri(), context.getConnectionId(), e.getStatus()
        );
      }
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

      var headers = HttpHeaders.headers().add("Content-Type", "application/json");
      if (e instanceof RetryAfterException retryAfter) {
        headers.add(
            RateLimitState.HEADER_RETRY_AFTER,
            Long.toString(retryAfter.getRetryAfter().toSeconds())
        );
      }
      context.setProxyResponseHeaders(headers);
    }

    return Future.succeededFuture(context);
  }

  /**
   * Map a request that got no answer to the response the caller sees: 504 with error code
   * {@code proxy_timeout} when Schema Registry didn't answer in time, and 500 otherwise. Only a
   * timeout that ended at {@code deadline} names it; an earlier one came from the client's own
   * connect or read timeout.
   */
  static Failure toFailure(ClusterProxyContext context, IOException e, Instant deadline) {
    var detail = Objects.requireNonNullElse(e.getMessage(), e.getClass().getSimpleName());
    if (e instanceof SocketTimeoutException) {
      var timeout = context.error("proxy_timeout", detail);
      return Instant.now().isBefore(deadline)
          ? timeout.failf(
              Response.Status.GATEWAY_TIMEOUT.getStatusCode(),
              "Schema Registry didn't respond in time"
          )
          : timeout.failf(
              Response.Status.GATEWAY_TIMEOUT.getStatusCode(),
              "Schema Registry didn't respond within %s",
              TimeUtil.humanReadableDuration(SCHEMA_REGISTRY_DEADLINE)
          );
    }
    return context
        .error("proxy_error", detail)
        .failf(
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            "Failed to process Schema Registry request: %s", detail
        );
  }
}
