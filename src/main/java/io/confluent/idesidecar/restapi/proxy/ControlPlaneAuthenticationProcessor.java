package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.connections.LocalConnectionState;
import io.confluent.idesidecar.restapi.connections.PlatformConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Processor to check if the request is authenticated. Checks for existence of control plane.
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
        // removed context.setProxyRequestHeaders(context.getRequestHeaders()); because .add probably takes care of it
        context.getRequestHeaders().add("Authorization", "Bearer " + controlPlaneToken);

      }
      case LocalConnectionState localConnection -> {
        // Do nothing
      }
      case DirectConnectionState directConnection -> {
        // TODO: DIRECT check auth status and fail if not connected/authenticated
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
