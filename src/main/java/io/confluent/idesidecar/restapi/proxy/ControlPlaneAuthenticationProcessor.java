package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.connections.LocalConnectionState;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
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
           CCloudApiProcessor.getControlPlaneToken(context, cCloudConnection);
      }
      case LocalConnectionState localConnection -> {
        // Do nothing
      }
      case DirectConnectionState directConnection -> {
        // Do nothing
      }
      default -> {
        // This should never happen
      }
    }

    return next().process(context);
  }
}