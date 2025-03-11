package io.confluent.idesidecar.restapi.proxy;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.connections.LocalConnectionState;
import io.confluent.idesidecar.restapi.connections.PlatformConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * This processor adds required authentication headers to interact with Confluent Cloud control
 * plane APIs.
 */
@ApplicationScoped
public class ControlPlaneAuthenticationProcessor extends
    Processor<ProxyContext, Future<ProxyContext>> {

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    var connectionState = context.getConnectionState();

    switch (connectionState) {
      case CCloudConnectionState cCloudConnection -> {
        var controlPlaneToken = cCloudConnection.getOauthContext().getControlPlaneToken();

        if (controlPlaneToken == null) {
          return Future.failedFuture(
              new ProcessorFailedException(context.fail(401, "Unauthorized")));
        }
        var headers = context.getProxyRequestHeaders() != null ? context.getProxyRequestHeaders()
            : MultiMap.caseInsensitiveMultiMap();
        headers.add(AUTHORIZATION, "Bearer %s".formatted(controlPlaneToken.token()));
        context.setProxyRequestHeaders(headers);
      }
      case LocalConnectionState localConnection -> {
        // Do nothing
      }
      case DirectConnectionState directConnection -> {
        // Do nothing
      }
      case PlatformConnectionState platformConnection -> {
        // Do nothing
      }
      default -> {
        // This should never happen
      }
    }

    return next().process(context);
  }
}
