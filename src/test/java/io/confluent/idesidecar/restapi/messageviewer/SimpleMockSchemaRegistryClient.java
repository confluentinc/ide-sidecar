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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleMockSchemaRegistryClient extends MockSchemaRegistryClient {

  private final Map<String, Map<ParsedSchema, String>> schemaCache;
  private final Map<Integer, ParsedSchema> idCache;
  private final Map<String, ParsedSchema> guidCache;
  private final AtomicInteger idCounter;
  private final Set<Integer> unauthenticated;
  private final Set<Integer> networkErrored;
  private final Set<String> unauthenticatedGuids;
  private final Set<String> networkErroredGuids;

  public SimpleMockSchemaRegistryClient() {
    this(List.of());
  }

  public SimpleMockSchemaRegistryClient(List<SchemaProvider> list) {
    super(list);
    schemaCache = new HashMap<>();
    idCache = new HashMap<>();
    guidCache = new HashMap<>();
    idCounter = new AtomicInteger(100001);
    unauthenticated = new HashSet<>();
    networkErrored = new HashSet<>();
    unauthenticatedGuids = new HashSet<>();
    networkErroredGuids = new HashSet<>();
  }

  public int register(int requiredSchemaId, String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, String.valueOf(requiredSchemaId));
    idCache.put(requiredSchemaId, schema);
    // Update the id counter to next number.
    idCounter.set(requiredSchemaId + 1);
    return requiredSchemaId;
  }

  /**
   * Registers a schema with both a GUID.
   */
  public String register(String guid, String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, guid);
    guidCache.put(guid, schema);
    return guid;
  }

  public SimpleMockSchemaRegistryClient registerWithStatusCode(int schemaId, int statusCode) {
    unauthenticated.add(schemaId);
    return this;
  }

  public SimpleMockSchemaRegistryClient registerWithStatusCode(String guid, int statusCode) {
    unauthenticatedGuids.add(guid);
    return this;
  }

  public SimpleMockSchemaRegistryClient registerAsNetworkErrored(int schemaId) {
    networkErrored.add(schemaId);
    return this;
  }

  public SimpleMockSchemaRegistryClient registerAsNetworkErrored(String guid) {
    networkErroredGuids.add(guid);
    return this;
  }

  @Override
  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    int id = idCounter.incrementAndGet();
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, String.valueOf(id));
    idCache.put(id, schema);
    return id;
  }

  @Override
  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    System.out.println("Hitting getSchemaById");
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
    Map<ParsedSchema, String> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null) {
      for (Map.Entry<ParsedSchema, String> entry : subjectSchemaCache.entrySet()) {
        if (entry.getValue().equals(String.valueOf(id))) {
          return entry.getKey();
        }
      }
    }
    return null;
  }

  @Override
  public ParsedSchema getSchemaByGuid(String guid, String format)
      throws IOException, RestClientException {
    System.out.println("Hitting getSchemaByGuid");
    if (unauthenticatedGuids.contains(guid)) {
      throw new RestClientException("User is denied operation on this server (mock).", 403, 40301);
    }
    if (networkErroredGuids.contains(guid)) {
      throw new ConnectException("Network error (mock)");
    }
    return guidCache.get(guid);
  }
}
