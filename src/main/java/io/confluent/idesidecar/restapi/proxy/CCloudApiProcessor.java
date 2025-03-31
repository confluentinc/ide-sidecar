package io.confluent.idesidecar.restapi.proxy;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CCloudApiProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Inject
  UriUtil uriUtil;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    String clusterId = context.getRequestHeaders().get("X-Cluster-Id");
    var connectionState = context.getConnectionState();
    if (connectionState instanceof CCloudConnectionState cCloudConnection
        && (clusterId == null
        || clusterId.isEmpty())) {
      if (getControlPlaneToken(context, cCloudConnection)) {
        return Future.failedFuture(
            new ProcessorFailedException(context.fail(401, "Unauthorized")));
      }
      context.setProxyRequestAbsoluteUrl(uriUtil.combine("https://api.confluent.cloud",
          context.getRequestUri()));
    } else {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Bad Request: Non-CCloud connections are not supported")));
    }
    return next().process(context);
  }

  static boolean getControlPlaneToken(
      ProxyContext context, CCloudConnectionState cCloudConnection) {
    var controlPlaneToken = cCloudConnection.getOauthContext().getControlPlaneToken();
    if (controlPlaneToken == null) {
      return true;
    }
    var headers = context.getProxyRequestHeaders() != null ? context.getProxyRequestHeaders()
        : MultiMap.caseInsensitiveMultiMap();
    headers.add(AUTHORIZATION, "Bearer %s".formatted(controlPlaneToken.token()));
    context.setProxyRequestHeaders(headers);
    return false;
  }
}