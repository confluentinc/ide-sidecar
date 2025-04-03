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
public class CCloudDataPlaneAuthenticationProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    var connectionState = context.getConnectionState();

    if (!(connectionState instanceof CCloudConnectionState cCloudConnection)) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Bad Request: Non-CCloud connections are not supported")));
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
}