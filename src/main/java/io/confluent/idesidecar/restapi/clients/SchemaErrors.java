package io.confluent.idesidecar.restapi.clients;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SchemaErrors {

  private static final Map<String, Map<String, String>> cacheOfCaches = new ConcurrentHashMap<>();

  private static String generateKey(String connectionId, String clusterId) {
    return connectionId + ":" + clusterId;
  }

  private String generateSchemaKey(String connectionId, int schemaId) {
    return connectionId + ":" + schemaId;
  }

  public static Map<String, String> getSubCache(String key) {
    return cacheOfCaches.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
  }

  // Methods for connection and cluster ID
  public String readClusterIdByConnectionId(String connectionId, String clusterId) {
    return getSubCache(generateKey(connectionId, clusterId)).get(clusterId);
  }

  public void writeClusterErrorByConnectionId(String connectionId, String clusterId, String error) {
    getSubCache(generateKey(connectionId, clusterId)).put(clusterId, error);
  }

  public static void clearClusterErrorByConnectionId(String connectionId, String clusterId) {
    Map<String, String> subCache = getSubCache(generateKey(connectionId, clusterId));
    subCache.remove(clusterId);
    if (subCache.isEmpty()) {
      cacheOfCaches.remove(generateKey(connectionId, clusterId));
    }
  }

  // Methods for schema ID
  public String readSchemaIdByConnectionId(String connectionId, int schemaId) {
    return getSubCache(generateSchemaKey(connectionId, schemaId)).get(String.valueOf(schemaId));
  }

  public void writeSchemaIdByConnectionId(String connectionId, int schemaId, String error) {
    getSubCache(generateSchemaKey(connectionId, schemaId)).put(String.valueOf(schemaId), error);
  }

  public void clearSchemaIdByConnectionId(String connectionId, int schemaId) {
    Map<String, String> subCache = getSubCache(generateSchemaKey(connectionId, schemaId));
    subCache.remove(String.valueOf(schemaId));
    if (subCache.isEmpty()) {
      cacheOfCaches.remove(generateSchemaKey(connectionId, schemaId));
    }
  }

  // Methods for connection ID
  public String readConnectionId(String connectionId) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(connectionId)) {
        return subCache.get(connectionId);
      }
    }
    return null;
  }

  public void writeConnectionIdError(String connectionId, String error) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(connectionId)){
        subCache.put(connectionId, error);
        return;
      }
    }
  }

  public void clearByConnectionId(String connectionId) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(connectionId)) {
        subCache.remove(connectionId);
        return;
      }
    }
  }
}