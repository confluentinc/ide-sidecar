package io.confluent.idesidecar.restapi.clients;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SchemaErrors {

  private static final Map<String, Map<String, String>> cacheOfCaches = new ConcurrentHashMap<>();

  public static Map<String, String> getSubCache(String key) {
    return cacheOfCaches.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
  }

  // Methods for schema ID
  public String readSchemaIdByConnectionId(String connectionId, int schemaId) {
    return getSubCache(connectionId).get(String.valueOf(schemaId));
  }

  public void writeSchemaIdByConnectionId(String connectionId, int schemaId, String error) {
    getSubCache(connectionId).put(String.valueOf(schemaId), error);
  }

  public void clearByConnectionId(String connectionId) {
    cacheOfCaches.remove(connectionId);
    return;
      }
    }