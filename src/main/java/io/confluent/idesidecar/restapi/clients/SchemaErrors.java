package io.confluent.idesidecar.restapi.clients;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SchemaErrors {

  private final Map<String, Map<String, String>> cacheOfCaches = new ConcurrentHashMap<>();

  private String generateKey(String connectionId, String clusterId) {
    return connectionId + ":" + clusterId;
  }

  private String generateSchemaKey(int schemaId) {
    return "schema:" + schemaId;
  }

  public Map<String, String> getSubCache(String key) {
    return cacheOfCaches.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
  }

  // Methods for connection and cluster ID
  public String readByConnectionId(String connectionId, String clusterId) {
    return getSubCache(generateKey(connectionId, clusterId)).get(clusterId);
  }

  public void writeByConnectionId(String connectionId, String clusterId, String error) {
    getSubCache(generateKey(connectionId, clusterId)).put(clusterId, error);
  }

  public void clearByConnectionId(String connectionId, String clusterId) {
    Map<String, String> subCache = getSubCache(generateKey(connectionId, clusterId));
    subCache.remove(clusterId);
    if (subCache.isEmpty()) {
      cacheOfCaches.remove(generateKey(connectionId, clusterId));
    }
  }

  // Methods for schema ID
  public String readBySchemaId(int schemaId) {
    return getSubCache(generateSchemaKey(schemaId)).get(String.valueOf(schemaId));
  }

  public void writeBySchemaId(int schemaId, String error) {
    getSubCache(generateSchemaKey(schemaId)).put(String.valueOf(schemaId), error);
  }

  public void clearBySchemaId(int schemaId) {
    Map<String, String> subCache = getSubCache(generateSchemaKey(schemaId));
    subCache.remove(String.valueOf(schemaId));
    if (subCache.isEmpty()) {
      cacheOfCaches.remove(generateSchemaKey(schemaId));
    }
  }

  // Methods for cluster ID
  public String readByClusterId(String clusterId) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(clusterId)) {
        return subCache.get(clusterId);
      }
    }
    return null;
  }

  public void writeByClusterId(String clusterId, String error) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(clusterId)) {
        subCache.put(clusterId, error);
        return;
      }
    }
  }

  public void clearByClusterId(String clusterId) {
    for (Map<String, String> subCache : cacheOfCaches.values()) {
      if (subCache.containsKey(clusterId)) {
        subCache.remove(clusterId);
        return;
      }
    }
  }
}