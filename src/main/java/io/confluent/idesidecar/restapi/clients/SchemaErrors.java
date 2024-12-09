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
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * The SchemaErrors class manages schema error caching for different connections.
 * It uses Caffeine cache to store and retrieve schema errors based on connection and schema IDs.
 *
 * <p>This class is application-scoped and observes connection lifecycle events to manage the cache.
 * It provides methods to read, write, and clear schema errors for specific connections.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>`ide-sidecar.schema-fetch-error-ttl`: The time-to-live duration for schema fetch errors in the cache.</li>
 * </ul>
 */
@ApplicationScoped
public class SchemaErrors {

  /**
   * Represents a unique connection identifier.
   */
  private record ConnectionId(String id) {

  }

  /**
   * Represents a unique schema identifier within a cluster.
   */
  private record SchemaId(String clusterId, int schemaId) {

  }

  /**
   * Represents an error message associated with a schema.
   */
  public record Error(String message) {

  }

  private static final Duration SCHEMA_FETCH_ERROR_TTL = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getValue(
              "ide-sidecar.schema-fetch-error-ttl",
              Long.class));

  private static final Map<ConnectionId, Cache<SchemaId, Error>> cacheOfCaches = new ConcurrentHashMap<>();

  /**
   * Retrieves or creates a sub-cache for a specific connection.
   *
   * @param key The connection identifier.
   * @return The sub-cache for the connection.
   */
  private Cache<SchemaId, Error> getSubCache(ConnectionId key) {
    return cacheOfCaches.computeIfAbsent(
        key,
        k -> Caffeine.newBuilder().expireAfterAccess(SCHEMA_FETCH_ERROR_TTL).build());
  }

  /**
   * Reads a schema error from the cache.
   *
   * @param connectionId The connection identifier.
   * @param schemaId     The schema identifier.
   * @return The schema error, or null if not found.
   */

  public Error readSchemaIdByConnectionId(String connectionId, String clusterId, int schemaId) {
    var cId = new ConnectionId(connectionId);
    var sId = new SchemaId(
        clusterId,
        schemaId
    );
    return getSubCache(cId).getIfPresent(sId);
  }

  /**
   * Writes a schema error to the cache.
   *
   * @param connectionId The connection identifier.
   * @param schemaId     The schema identifier.
   * @param error        The schema error.
   */
  public void writeSchemaIdByConnectionId(
      String connectionId,
      int schemaId,
      String clusterId,
      Error error
  ) {
    var cId = new ConnectionId(connectionId);
    var sId = new SchemaId(
        clusterId,
        schemaId
    );
    getSubCache(cId).put(sId, error);
  }

  /**
   * Clears the cache for a specific connection.
   *
   * @param connectionId The connection identifier.
   */
  public void clearByConnectionId(
      String connectionId
  ) {
    var cId = new ConnectionId(connectionId);
    cacheOfCaches.remove(cId);
  }

  /**
   * Removes the cache by connections id in response to lifecycle events.
   *
   * @param connection The connection state.
   */
  public void onConnectionChange(@ObservesAsync @Lifecycle.Deleted @Lifecycle.Created @Lifecycle.Updated ConnectionState connection) {
    var cId = new ConnectionId(connection.getId());
    cacheOfCaches.remove(cId);
  }
}