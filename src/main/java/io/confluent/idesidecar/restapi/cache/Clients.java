package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.quarkus.logging.Log;
import jakarta.enterprise.event.ObservesAsync;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Utilities to obtain and cache clients for a given connection and client ID.
 * Client instances of {@code T} are cached and retrieved by the client ID.
 * You may get creative with this and pass any unique identifier for the client.
 */
public abstract class Clients<T extends AutoCloseable> {

  /**
   * Caffeine spec to use for the client cache. By default, this will be an empty spec.
   * See the
   * <a href="https://github.com/ben-manes/caffeine/wiki/Specification">Caffeine Spec</a>
   * for more information on the format. Inherited classes may override this method
   * and return a different spec.
   */
  private final CaffeineSpec caffeineSpec;

  /**
   * Store an instance of a Caffeine cache for each connection. The cache will store
   * clients by client ID and its policy may be configured by the {@link #caffeineSpec}.
   */
  private final Map<String, Cache<String, T>> clientsByIdByConnections =
      new ConcurrentHashMap<>();

  protected Clients(CaffeineSpec caffeineSpec) {
    this.caffeineSpec = caffeineSpec;
  }

  protected Clients() {
    this(CaffeineSpec.parse(""));
  }

  /**
   * Get a client for the given connection and client ID. If the client does not
   * already exist, it will be created using the provided factory.
   *
   * @param connectionId     the ID of the connection
   * @param clientId         the identifier of the client.
   * @param factory          the method that will create the client if there is not already one
   * @return the client
   */
  public T getClient(
      String connectionId,
      String clientId,
      Supplier<T> factory
  ) {
    return clientsForConnection(connectionId).asMap().computeIfAbsent(
        clientId,
        k -> factory.get()
    );
  }

  private Cache<String, T> clientsForConnection(String connectionId) {
    return clientsByIdByConnections.computeIfAbsent(connectionId, k -> createCache());
  }

  int clientCount() {
    return clientsByIdByConnections
        .values()
        .stream()
        .map(Cache::asMap)
        .map(Map::size)
        .mapToInt(Integer::intValue)
        .sum();
  }

  int clientCount(String connectionId) {
    return clientsForConnection(connectionId).asMap().size();
  }

  void clearClients(String connectionId) {
    var oldCache = clientsByIdByConnections.put(connectionId, createCache());
    if (oldCache != null) {
      // Invalidation will trigger the removal listener, which will close the clients
      oldCache.invalidateAll();
    }
  }

  void handleRemoval(String key, T value, RemovalCause cause) {
    try {
      if (value != null) {
        Log.debugf("Closing client %s", value);
        value.close();
      }
    } catch (Throwable t) {
      Log.debugf("Error closing client %s: %s", value, t);
      // Ignore these as we don't care
    }
  }

  private Cache<String, T> createCache() {
    return Caffeine
        .from(caffeineSpec)
        .removalListener(this::handleRemoval)
        .build();
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

  /**
   * Respond to the connection being updated by clearing and closing the
   * clients that were cached for that connection. This ensures that the clients
   * don't use stale connection information.
   * @param connection the connection that was updated
   */
  void onConnectionUpdated(@ObservesAsync @Lifecycle.Updated ConnectionState connection) {
    clearClients(connection.getId());
  }
}
