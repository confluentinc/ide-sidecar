package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleMockSchemaRegistryClient extends MockSchemaRegistryClient {
  private final Map<String, Map<ParsedSchema, Integer>> schemaCache;
  private final Map<Integer, ParsedSchema> idCache;
  private final AtomicInteger idCounter;
  private final Set<Integer> unauthenticated;
  private final Set<Integer> networkErrored;

  public SimpleMockSchemaRegistryClient() {
    this(List.of());
  }

  public SimpleMockSchemaRegistryClient(List<SchemaProvider> list) {
    super(list);
    schemaCache = new HashMap<>();
    idCache = new HashMap<>();
    idCounter = new AtomicInteger(100001);
    unauthenticated = new HashSet<>();
    networkErrored = new HashSet<>();
  }

  public int register(int requiredSchemaId, String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, requiredSchemaId);
    idCache.put(requiredSchemaId, schema);
    // Update the id counter to next number.
    idCounter.set(requiredSchemaId + 1);
    return requiredSchemaId;
  }

  public SimpleMockSchemaRegistryClient registerWithStatusCode(int schemaId, int statusCode) {
    unauthenticated.add(schemaId);
    return this;
  }

  public SimpleMockSchemaRegistryClient registerAsNetworkErrored(int schemaId) {
    networkErrored.add(schemaId);
    return this;
  }

  @Override
  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    int id = idCounter.incrementAndGet();
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, id);
    idCache.put(id, schema);
    return id;
  }

  @Override
  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    if (unauthenticated.contains(id)) {
      throw new RestClientException("User is denied operation on this server (mock).", 403, 40301);
    }
    if (networkErrored.contains(id)) {
      // Could be any network error, but throwing ConnectException since it is a common one.
      throw new ConnectException("Network error (mock)");
    }
    return idCache.get(id);
  }

  @Override
  public ParsedSchema getSchemaBySubjectAndId(String subject, int id) {
    Map<ParsedSchema, Integer> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null) {
      for (Map.Entry<ParsedSchema, Integer> entry : subjectSchemaCache.entrySet()) {
        if (entry.getValue() == id) {
          return entry.getKey();
        }
      }
    }
    return null;
  }
}
