package io.confluent.idesidecar.restapi.proxy;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ControlPlaneTokenNotFoundException;
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

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.control-plane-base-url")
  String ccloudApiBaseUrl;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    String clusterId = context.getRequestHeaders().get("X-Cluster-Id");
    var connectionState = context.getConnectionState();
    if (clusterId != null && !clusterId.isEmpty()) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "This route does not support the request header 'X-Cluster-Id'."
                  + " Please remove the header and try again.")));
    } else if (connectionState instanceof CCloudConnectionState cCloudConnection) {
        try {
          getControlPlaneToken(context, cCloudConnection);
        } catch (ControlPlaneTokenNotFoundException e) {;
          return Future.failedFuture(
              new ProcessorFailedException(
                  context.fail(401, "%s".formatted(e.getMessage()))));
        }
        context.setProxyRequestAbsoluteUrl(uriUtil.combine(ccloudApiBaseUrl,
            context.getRequestUri()));
        return next().process(context);
    } else {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Bad Request: Non-CCloud connections are not supported")));
    }
  }

  /**
   * Adds the control plane token to the provided context's request headers or returns a failed Future
   * with a CCloudAuthenticationFailedException if the token is missing.
   *
   * @param context The proxy context to add the authorization header to
   * @param cCloudConnection The CCloud connection state containing the OAuth context with the token
   */
  static void getControlPlaneToken(
      ProxyContext context, CCloudConnectionState cCloudConnection) {
    var controlPlaneToken = cCloudConnection.getOauthContext().getControlPlaneToken();

    // If token is missing, return a failed future with ControlPlaneTokenNotFoundException
    if (controlPlaneToken == null) {
    throw new ControlPlaneTokenNotFoundException("Control plane token not found");
    }

    // Token exists, add it to the headers
    var headers = context.getProxyRequestHeaders() != null ? context.getProxyRequestHeaders()
        : MultiMap.caseInsensitiveMultiMap();
    headers.add(AUTHORIZATION, "Bearer %s".formatted(controlPlaneToken.token()));
    context.setProxyRequestHeaders(headers);

  }
}