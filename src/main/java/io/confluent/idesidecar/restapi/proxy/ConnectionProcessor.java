package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.quarkus.logging.Log;
import io.vertx.core.Future;

/**
 * Processor that checks for the `x-connection-id` header and retrieves the connection state from
 * the connection state manager.
 *
 * @param <T> The proxy context type, which must extend {@link ProxyContext}
 */
public class ConnectionProcessor<T extends ProxyContext> extends
    Processor<T, Future<T>> {

  ConnectionStateManager connectionStateManager;

  public ConnectionProcessor(ConnectionStateManager connectionStateManager) {
    this.connectionStateManager = connectionStateManager;
  }

  @Override
  public Future<T> process(T context) {
    Log.info("Start ConnectionProcessor");
    // First, check for the connection id header
    var connectionId = context.getConnectionId();
    if (connectionId == null) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.failf(
                  400,
                  "%s header is required",
                  RequestHeadersConstants.CONNECTION_ID_HEADER)
          )
      );
    }

    // Ok, looks good, now see if we know about this connection
    ConnectionState connectionState;
    try {
      connectionState = connectionStateManager.getConnectionState(connectionId);
    } catch (ConnectionNotFoundException e) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.failf(404, "Connection id=%s not found", connectionId))
      );
    }
    // Store the connection details in the context
    context.setConnectionState(connectionState);

    // All right, we may now proceed
    Log.info("End ConnectionProcessor");
    return next().process(context);
  }
}
