/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Utilities to obtain and cache Schema Registry clients.
 */
@ApplicationScoped
public class SchemaRegistryClients {

  private final Map<String, Map<String, SchemaRegistryClient>> clientsByIdByConnections =
      new ConcurrentHashMap<>();

  /**
   * Get a client for the Schema Registry with the given identifier.
   *
   * @param connectionId     the ID of the connection
   * @param schemaRegistryId the identifier of the Schema Registry
   * @param factory          the method that will create the client if there is not already one
   * @return the Schema Registry client
   */
  public SchemaRegistryClient getClient(
      String connectionId,
      String schemaRegistryId,
      Supplier<SchemaRegistryClient> factory
  ) {
    return clientsForConnection(connectionId).computeIfAbsent(
        schemaRegistryId,
        k -> factory.get()
    );
  }

  private Map<String, SchemaRegistryClient> clientsForConnection(String connectionId) {
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
   * Schema Registry clients that were cached for that connection.
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
