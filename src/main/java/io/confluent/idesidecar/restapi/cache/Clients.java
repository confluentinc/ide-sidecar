package io.confluent.idesidecar.restapi.cache;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import jakarta.enterprise.event.ObservesAsync;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Utilities to obtain and cache clients for a given connection and client ID.
 */
public abstract class Clients<T extends AutoCloseable> {

  private final Map<String, Map<String, T>> clientsByIdByConnections =
      new ConcurrentHashMap<>();

  /**
   * Get a client for the given connection and client ID. If the client does not
   * already exist, it will be created using the provided factory.
   *
   * @param connectionId     the ID of the connection
   * @param clientId         the identifier of the client
   * @param factory          the method that will create the client if there is not already one
   * @return the client
   */
  public T getClient(
      String connectionId,
      String clientId,
      Supplier<T> factory
  ) {
    return clientsForConnection(connectionId).computeIfAbsent(
        clientId,
        k -> factory.get()
    );
  }

  private Map<String, T> clientsForConnection(String connectionId) {
    return clientsByIdByConnections.computeIfAbsent(
        connectionId,
        k -> new ConcurrentHashMap<>()
    );
  }

  int clientCount() {
    return clientsByIdByConnections
        .values()
        .stream()
        .map(Map::size)
        .mapToInt(Integer::intValue)
        .sum();
  }

  int clientCount(String connectionId) {
    return clientsForConnection(connectionId).size();
  }

  void clearClients(String connectionId) {
    var oldCache = clientsByIdByConnections.put(connectionId, new ConcurrentHashMap<>());
    if (oldCache != null) {
      // clean up all clients in the old cache
      oldCache.forEach((id, client) -> {
        try {
          client.close();
        } catch (Throwable t) {
          // Ignore these as we don't care
        }
      });
    }
  }

  /**
   * Respond to the connection being disconnected by clearing and closing the
   * clients that were cached for that connection.
   *
   * @param connection the connection that was disconnected
   */
  void onConnectionDisconnected(
      @ObservesAsync @Lifecycle.Disconnected ConnectionState connection
  ) {
    clearClients(connection.getId());
  }

  /**
   * Respond to the connection being deleted by clearing and closing the
   * Schema Registry clients that were cached for that connection.
   *
   * @param connection the connection that was deleted
   */
  void onConnectionDeleted(@ObservesAsync @Lifecycle.Deleted ConnectionState connection) {
    clearClients(connection.getId());
  }
}
