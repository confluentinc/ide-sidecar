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
  private final ProxyHttpClient<ClusterProxyContext> proxyHttpClient;

  @Inject
  public ClusterProxyRequestProcessor(WebClientFactory webClientFactory, Vertx vertx) {
    proxyHttpClient = new ProxyHttpClient<>(webClientFactory, vertx);
  }

  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    return proxyHttpClient.send(context).compose(
        processedContext -> next().process(processedContext)
    );
  }
}
