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

@ApplicationScoped
public class SchemaErrors {

  public record ConnectionId(String id) {

  }

  public record SchemaId(String clusterId, int schemaId) {

  }

  public record Error(String message) {

  }

  private static final Duration SCHEMA_FETCH_ERROR_TTL = Duration.ofSeconds(30);

  public static final Map<ConnectionId, Cache<SchemaId, SchemaErrors.Error>> cacheOfCaches = new ConcurrentHashMap<>();

  public static Cache<SchemaId, Error> getSubCache(ConnectionId key) {
    return cacheOfCaches.computeIfAbsent(key, k -> Caffeine.newBuilder()
                                                           .expireAfterAccess(
                                                               SCHEMA_FETCH_ERROR_TTL)
                                                           .build());
  }

  // Methods for schema ID
  public Error readSchemaIdByConnectionId(
      ConnectionId connectionId,
      SchemaId schemaId
  ) {
    return getSubCache(connectionId).getIfPresent(schemaId);
  }

  public void writeSchemaIdByConnectionId(
      ConnectionId connectionId,
      SchemaId schemaId,
      Error error
  ) {
    getSubCache(connectionId).put(schemaId, error);
  }

  public void clearByConnectionId(
      ConnectionId connectionId
  ) {
    cacheOfCaches.remove(connectionId);
  }

  public void onConnectionChange(@ObservesAsync @Lifecycle.Deleted @Lifecycle.Created @Lifecycle.Updated ConnectionState connection) {
    cacheOfCaches.remove(new ConnectionId(connection.getId()));
  }
}