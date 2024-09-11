package io.confluent.idesidecar.restapi.messageviewer;


import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
  private Set<Integer> unauthenticated;

  public SimpleMockSchemaRegistryClient() {
    schemaCache = new HashMap<>();
    idCache = new HashMap<>();
    idCounter = new AtomicInteger(100001);
    unauthenticated = new HashSet<>();
  }

  public SimpleMockSchemaRegistryClient(List<SchemaProvider> list) {
    super(list);
    schemaCache = new HashMap<>();
    idCache = new HashMap<>();
    idCounter = new AtomicInteger(100001);
    unauthenticated = new HashSet<>();
  }

  public int register(int requiredSchemaId, String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    schemaCache.computeIfAbsent(subject, k -> new HashMap<>()).put(schema, requiredSchemaId);
    idCache.put(requiredSchemaId, schema);
    // Update the id counter to next number.
    idCounter.set(requiredSchemaId + 1);
    return requiredSchemaId;
  }

  public void registerUnAuthenticated(int schemaId) {
    unauthenticated.add(schemaId);
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
      throw new RestClientException("User is denied operation on this server.", 403, 40301);
    }
    return idCache.get(id);
  }

  @Override
  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
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

  @Override
  public Collection<String> getAllSubjectsById(int id) {
    Collection<String> subjects = new ArrayList<>();
    for (Map.Entry<String, Map<ParsedSchema, Integer>> entry : schemaCache.entrySet()) {
      if (entry.getValue().containsValue(id)) {
        subjects.add(entry.getKey());
      }
    }
    return subjects;
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
    Map<ParsedSchema, Integer> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null && !subjectSchemaCache.isEmpty()) {
      int latestVersion = Collections.max(subjectSchemaCache.values());
      ParsedSchema latestSchema = getSchemaBySubjectAndId(subject, latestVersion);
      return new SchemaMetadata((Schema) latestSchema);
    }
    return null;
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
    ParsedSchema schema = getSchemaBySubjectAndId(subject, version);
    if (schema != null) {
      return new SchemaMetadata((Schema) schema);
    }
    return null;
  }

  @Override
  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    Map<ParsedSchema, Integer> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null) {
      return subjectSchemaCache.getOrDefault(schema, -1);
    }
    return -1;
  }

  @Override
  public List<Integer> getAllVersions(String subject) throws IOException, RestClientException {
    Map<ParsedSchema, Integer> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null) {
      return new ArrayList<>(subjectSchemaCache.values());
    }
    return Collections.emptyList();
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return false; // Implement if needed for the test
  }

  @Override
  public String setMode(String mode) throws IOException, RestClientException {
    return null; // Implement if needed for the test
  }

  @Override
  public String setMode(String mode, String subject) throws IOException, RestClientException {
    return null; // Implement if needed for the test
  }

  @Override
  public String getMode() throws IOException, RestClientException {
    return null; // Implement if needed for the test
  }

  @Override
  public String getMode(String subject) throws IOException, RestClientException {
    return null; // Implement if needed for the test
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return schemaCache.keySet();
  }

  @Override
  public int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
    Map<ParsedSchema, Integer> subjectSchemaCache = schemaCache.get(subject);
    if (subjectSchemaCache != null) {
      return subjectSchemaCache.getOrDefault(schema, -1);
    }
    return -1;
  }

  @Override
  public void reset() {
    schemaCache.clear();
    idCache.clear();
    idCounter.set(100001);
  }
}
