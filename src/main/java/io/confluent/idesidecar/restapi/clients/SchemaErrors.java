package io.confluent.idesidecar.restapi.clients;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The `SchemaErrors` class manages schema error caching for different connections.
 * It uses Caffeine cache to store and retrieve schema errors based on connection and schema IDs.
 *
 * <p>This class is application-scoped and observes connection lifecycle events to manage the cache.
 * It provides methods to read, write, and clear schema errors for specific connections.
 */

@ApplicationScoped
public class SchemaErrors {

  // `ConnectionId` represents a unique connection identifier.
  public record ConnectionId(String id) {

  }

  // `SchemaId` represents a unique schema identifier within a cluster.
  public record SchemaId(String clusterId, int schemaId) {

  }

  //`Error` represents an error message associated with a schema.
  public record Error(String message) {

  }
  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  // The time-to-live duration for schema fetch errors in the cache.
  @ConfigProperty(name = "ide-sidecar.schema-fetch-error-ttl")
  long schemaFetchErrorTtl;

  public static final Map<ConnectionId, Cache<SchemaId, SchemaErrors.Error>> cacheOfCaches = new ConcurrentHashMap<>();

  // Retrieves or creates a sub-cache for a specific connection.
  public Cache<SchemaId, Error> getSubCache(ConnectionId key) {
    return cacheOfCaches.computeIfAbsent(
        key,
        k -> Caffeine.newBuilder().expireAfterAccess(Duration.ofSeconds(schemaFetchErrorTtl)).build());
  }

  // Reads a schema error from the cache using both the connection id and the schema id.
  public Error readSchemaIdByConnectionId(
      ConnectionId connectionId,
      SchemaId schemaId
  ) {
    return getSubCache(connectionId).getIfPresent(schemaId);
  }

  // Writes a schema error to the cache using both the connection id and the schema id.
  public void writeSchemaIdByConnectionId(
      ConnectionId connectionId,
      SchemaId schemaId,
      Error error
  ) {
    getSubCache(connectionId).put(schemaId, error);
  }

  // Clears the cache for a specific connection.
  public void clearByConnectionId(
      ConnectionId connectionId
  ) {
    cacheOfCaches.remove(connectionId);
  }

  // Removes the cache by connections id in response to lifecycle events.
  public void onConnectionChange(@ObservesAsync @Lifecycle.Deleted @Lifecycle.Created @Lifecycle.Updated ConnectionState connection) {
    cacheOfCaches.remove(new ConnectionId(connection.getId()));
  }
}