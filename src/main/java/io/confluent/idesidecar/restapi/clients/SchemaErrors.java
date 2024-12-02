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

@ApplicationScoped
public class SchemaErrors {

  public record ConnectionId(String id) {

  }

  public record SchemaId(String clusterId, int schemaId) {

  }

  public record Error(String message) {

  }

  private static Duration schemaFetchErrorTtl;

  @ConfigProperty(name = "ide-sidecar.schema-fetch-error-ttl")
  long getSchemaFetchErrorTtl;

  public static final Map<ConnectionId, Cache<SchemaId, SchemaErrors.Error>> cacheOfCaches = new ConcurrentHashMap<>();

  public static Cache<SchemaId, Error> getSubCache(ConnectionId key) {
    return cacheOfCaches.computeIfAbsent(
        key,
        k -> Caffeine.newBuilder().expireAfterAccess(schemaFetchErrorTtl).build());
  }

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