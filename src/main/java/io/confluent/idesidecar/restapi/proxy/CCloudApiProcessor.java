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
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class CCloudApiProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Inject
  UriUtil uriUtil;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.api-base-url")
  String ccloudApiBaseUrl;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    String clusterId = context.getRequestHeaders().get("X-Cluster-Id");
    var connectionState = context.getConnectionState();
    if (connectionState instanceof CCloudConnectionState cCloudConnection) {
      return getControlPlaneToken(context, cCloudConnection).compose(v -> {
        context.setProxyRequestAbsoluteUrl(uriUtil.combine(ccloudApiBaseUrl,
            context.getRequestUri()));
        return next().process(context);
      });
    } else if (clusterId != null && !clusterId.isEmpty()) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "This route does not support the request header 'X-Cluster-Id'."
                  + " Please remove the header and try again.")));
    } else {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Bad Request: Non-CCloud connections are not supported")));
    }
  }

  static Future getControlPlaneToken(
      ProxyContext context, CCloudConnectionState cCloudConnection) {
    var controlPlaneToken = cCloudConnection.getOauthContext().getControlPlaneToken();
    if (controlPlaneToken == null) {
      return Future.failedFuture(
          new ProcessorFailedException(context.fail(401, "Not authenticated with Confluent Cloud")));
    }
    var headers = context.getProxyRequestHeaders() != null ? context.getProxyRequestHeaders()
        : MultiMap.caseInsensitiveMultiMap();
    headers.add(AUTHORIZATION, "Bearer %s".formatted(controlPlaneToken.token()));
    context.setProxyRequestHeaders(headers);
    return Future.succeededFuture();
  }
}