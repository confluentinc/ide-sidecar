package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.connections.LocalConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.Future;

/**
 * Processor to check if the cluster request is authenticated. Checks for existence of data plane
 * token in case of Confluent Cloud clusters.
 */
public class ClusterAuthenticationProcessor<T extends ClusterProxyContext> extends
    Processor<T, Future<T>> {

  @Override
  public Future<T> process(T context) {
    var connectionState = context.getConnectionState();

    switch (connectionState) {
      case CCloudConnectionState cCloudConnection -> {
        var dataPlaneToken = cCloudConnection.getOauthContext().getDataPlaneToken();
        if (dataPlaneToken == null) {
          return Future.failedFuture(
              new ProcessorFailedException(context.fail(401, "Unauthorized")));
        }
      }
      case LocalConnectionState localConnection -> {
        // Do nothing
      }
      case DirectConnectionState directConnection -> {
        // TODO: DIRECT check auth status and fail if not connected/authenticated
      }
      default -> {
        // This should never happen
      }
    }

    return next().process(context);
  }
}
