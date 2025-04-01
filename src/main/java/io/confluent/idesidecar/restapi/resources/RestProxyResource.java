package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.web.Route;
import io.smallrye.common.annotation.Blocking;
import io.swagger.v3.oas.annotations.Operation;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

/**
 * Resource that proxies requests to the Kafka REST and Schema Registry APIs.
 */
@ApplicationScoped
public class RestProxyResource {

  public static final String KAFKA_PROXY_REGEX = "/kafka/v3/clusters/(?<clusterId>[^\\/]+).*";
  private static final String CLUSTER_ID_PATH_PARAM = "clusterId";
  public static final String SCHEMA_REGISTRY_PROXY_REGEX = "(/schemas.*)|(/subjects.*)";
  public static final String RBAC_RESOURCE_PATH = "/api/metadata/security/v2alpha1/authorize";
  static final String RBAC_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.rbac-uri", String.class);
  static final MultiMap NO_HEADERS = MultiMap.caseInsensitiveMultiMap();
  static final Map<String, String> NO_PATH_PARAMS = Map.of();

  // Add this static declaration alongside the other static constants
  static final String ccloudApiProxyRegex = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.proxy.ccloud-api-control-plane-regex", String.class);

  @Inject
  Router router;

  void init(@Observes StartupEvent ev) {
    router.routeWithRegex(ccloudApiProxyRegex)
        .handler(BodyHandler.create())
        .blockingHandler(this::ccloudProxy);
  }

  @Inject
  @Named("clusterProxyProcessor")
  Processor<ClusterProxyContext, Future<ClusterProxyContext>> clusterProxyProcessor;

  @Inject
  @Named("RBACProxyProcessor")
  Processor<ProxyContext, Future<ProxyContext>> rbacProxyProcessor;

  @Inject
  @Named("ControlPlaneProxyProcessor")
  Processor<ProxyContext, Future<ProxyContext>> genericProxyProcessor;

  @Inject
  @Named("CCloudProxyProcessor")
  Processor<ProxyContext, Future<ProxyContext>> ccloudProxyProcessor;

  @Route(regex = KAFKA_PROXY_REGEX)
  @Blocking
  public void kafkaClusterProxy(RoutingContext routingContext) {
    handleClusterProxy(routingContext, createKafkaClusterContext(routingContext));
  }

  @Route(regex = SCHEMA_REGISTRY_PROXY_REGEX)
  @Blocking
  public void schemaRegistryClusterProxy(RoutingContext routingContext) {
    handleClusterProxy(routingContext, createSRClusterContext(routingContext));
  }

  private void handleClusterProxy(RoutingContext routingContext, ClusterProxyContext proxyContext) {
    process(routingContext, clusterProxyProcessor, proxyContext);
  }

  private void handleRBACProxy(RoutingContext routingContext, ProxyContext proxyContext) {
    process(routingContext, rbacProxyProcessor, proxyContext);
  }

  private void handleControlPlaneProxy(RoutingContext routingContext, ProxyContext proxyContext) {
    process(routingContext, genericProxyProcessor, proxyContext);
  }

  private void ccloudProxy(RoutingContext routingContext) {
    process(routingContext, ccloudProxyProcessor, createCcloudProxyContext(routingContext));
  }

  public record RBACRequest(
      String userPrincipal,
      Action[] actions
  ) {

    public record Action(
        String resourceType,
        String resourceName,
        String operation,
        Scope scope
    ) {

      public record Scope(
          Map<String, String> clusters,
          String[] path
      ) {

      }
    }
  }

  @Route(
      path = RBAC_RESOURCE_PATH,
      methods = Route.HttpMethod.PUT,
      produces = MediaType.APPLICATION_JSON,
      consumes = MediaType.APPLICATION_JSON
  )
  @Blocking
  @Operation(summary = "RBAC proxy route", description = "Proxy route for RBAC requests")
  @RequestBody(
      description = "RBAC request body",
      required = true,
      content = @Content(schema = @Schema(implementation = RBACRequest.class))
  )
  @APIResponses({
      @APIResponse(
          responseCode = "200",
          description = "Successful response",
          content = @Content(schema = @Schema(implementation = String[].class))
      ),
  })
  public void rbacProxyRoute(RoutingContext routingContext) {
    handleRBACProxy(routingContext, createRBACProxyContext(routingContext));
  }

  public void controlPlaneProxyRoute(RoutingContext routingContext) {
    handleControlPlaneProxy(routingContext, createControlPlaneProxyContext(routingContext));
  }

  private <T extends ProxyContext> void process(RoutingContext routingContext,
      Processor<T, Future<T>> processor, T proxyContext) {
    processor.process(proxyContext)
        .onSuccess(context -> {
          routingContext.response().setStatusCode(context.getProxyResponseStatusCode());
          if (context.getProxyResponseHeaders() != null) {
            routingContext.response().headers().addAll(context.getProxyResponseHeaders());
          }
          if (context.getProxyResponseBody() != null) {
            // Set content-length header to the length of the response body
            // so that the client knows when the response is complete.
            // Set only if transfer-encoding is not set, as it takes precedence.
            if (context.getProxyResponseHeaders().get(HttpHeaders.TRANSFER_ENCODING) == null) {
              routingContext.response().putHeader(
                  HttpHeaders.CONTENT_LENGTH,
                  String.valueOf(context.getProxyResponseBody().getBytes().length)
              );
            }
            routingContext.response().end(context.getProxyResponseBody());
          } else {
            routingContext.response().end();
          }
        }).onFailure(throwable -> {
          var failure = ((ProcessorFailedException) throwable).getFailure();
          routingContext.response().setStatusCode(Integer.parseInt(failure.status()));
          routingContext.response()
              .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
          routingContext.response().send(failure.asJsonString());
        });
  }

/*
   DEV NOTES:
   We create a ClusterProxyContext from the given RoutingContext. This makes it such that we don't
   have to store the RoutingContext itself in the ClusterProxyContext, which would give it more
   power and information than it needs. For example, a processor with access to the
   RoutingContext would be able to send a response and end the request, which would break
   assumptions made by other processors.
   */

  /**
   * Create a ClusterProxyContext for Kafka Proxy. Note that we are using the cluster id from the
   * path params, e.g, /kafka/v3/clusters/{clusterId}/topics/{topicName}
   */
  private ClusterProxyContext createKafkaClusterContext(RoutingContext routingContext) {
    return new ClusterProxyContext(
        routingContext.request().uri(),
        routingContext.request().headers(),
        routingContext.request().method(),
        routingContext.body().buffer(),
        routingContext.pathParams(),
        routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER),
        routingContext.pathParams().get(CLUSTER_ID_PATH_PARAM),
        ClusterType.KAFKA
    );
  }

  /**
   * Create a ClusterProxyContext for Schema Registry Proxy. Note that we are using the cluster id
   * from the {@code x-cluster-id} header.
   */
  private ClusterProxyContext createSRClusterContext(RoutingContext routingContext) {
    return new ClusterProxyContext(
        routingContext.request().uri(),
        routingContext.request().headers(),
        routingContext.request().method(),
        routingContext.body().buffer(),
        routingContext.pathParams(),
        routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER),
        routingContext.request().getHeader(RequestHeadersConstants.CLUSTER_ID_HEADER),
        ClusterType.SCHEMA_REGISTRY
    );
  }

  private ProxyContext createRBACProxyContext(RoutingContext routingContext) {
    return new ProxyContext(
        RBAC_URI,
        NO_HEADERS,
        HttpMethod.PUT,
        routingContext.body().buffer(),
        NO_PATH_PARAMS,
        routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER)
    );
  }

  private ProxyContext createControlPlaneProxyContext(RoutingContext routingContext) {
    return new ProxyContext(
        routingContext.request().uri(),
        routingContext.request().headers(),
        routingContext.request().method(),
        routingContext.body().buffer(),
        routingContext.pathParams(),
        routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER)
    );
  }

  private ProxyContext createCcloudProxyContext(RoutingContext routingContext) {
    return new ProxyContext(
        routingContext.request().uri(),
        routingContext.request().headers(),
        routingContext.request().method(),
        routingContext.body().buffer(),
        routingContext.pathParams(),
        routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER)
    );
  }
}