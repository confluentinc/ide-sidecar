package io.confluent.idesidecar.restapi.proxy;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Processor for handling data plane requests to services like Flink SQL and Catalog.
 */
@ApplicationScoped
public class FlinkDataPlaneAuthenticationProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    var connectionState = context.getConnectionState();

    if (!(connectionState instanceof CCloudConnectionState cCloudConnection)) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Bad Request: Non-CCloud connections are not supported")));
    }

    // Check if this is a data plane request
    if (!isDataPlaneRequest(context)) {
      // Skip non-data plane requests
      return next().process(context);
    }

    // Get the proper Confluent Cloud API token from the connection state
    if (cCloudConnection.getOauthContext() == null ||
        cCloudConnection.getOauthContext().getDataPlaneToken() == null) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(401, "Data plane token not found")));
    }

    String ccloudToken = cCloudConnection.getOauthContext().getDataPlaneToken().token();

    // Set data plane authentication
    var headers = context.getProxyRequestHeaders() != null ?
        context.getProxyRequestHeaders() : MultiMap.caseInsensitiveMultiMap();
    headers.remove("authorization");
    headers.add(AUTHORIZATION, "Bearer %s".formatted(ccloudToken));
    context.setProxyRequestHeaders(headers);

    return next().process(context);
  }

  /**
   * Determines whether the request is for a data plane service.
   *
   * @param context The proxy context containing the request URI
   * @return true if the request is for a data plane service
   */
  private boolean isDataPlaneRequest(ProxyContext context) {
    return context.getRequestUri().contains("sql/v1");
  }
}