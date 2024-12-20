/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.queryGraphQLRaw;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static java.util.function.Predicate.not;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.credentials.Redactable;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.LocalConfig;
import io.confluent.idesidecar.restapi.models.ConnectionsList;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.quarkus.logging.Log;
import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

public class SidecarClient implements SidecarClientApi {

  private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger(0);
  private static final String CONNECTION_ID_TEMPLATE = "test-connection-%d";

  private static final Integer TEST_PORT = ConfigProvider
      .getConfig()
      .getValue("quarkus.http.test-port", Integer.class);

  private static final String SIDECAR_HOST = "http://localhost:%s".formatted(TEST_PORT);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String sidecarHost;

  private String currentConnectionId;
  private String currentClusterId;
  private String currentKafkaClusterId;
  private String currentSchemaClusterId;
  private Set<KafkaCluster> usedKafkaClusters = new HashSet<>();
  private Set<SchemaRegistry> usedSchemaRegistries = new HashSet<>();

  static {
    // Used to serialize Password objects into their raw value
    // when sending them in the request body
    OBJECT_MAPPER.registerModule(
        new SimpleModule().addSerializer(Password.class, new PasswordSerializer())
    );
  }

  public SidecarClient() {
    this.sidecarHost = SIDECAR_HOST;
  }

  public String sidecarHost() {
    return sidecarHost;
  }

  public void createTopic(String topicName, int partitions, int replicationFactor) {
    withCluster(currentKafkaClusterId, () -> {
      var resp = givenDefault()
          .body(
              CreateTopicRequestData
                  .builder()
                  .topicName(topicName)
                  .partitionsCount(partitions)
                  .replicationFactor(replicationFactor)
                  .build()
          )
          .post("/kafka/v3/clusters/{cluster_id}/topics")
          .then().extract().response();

      if (resp.statusCode() != 200) {
        fail("Failed to create topic: Status: %d, message: %s".formatted(resp.statusCode(), resp.body().asString()));
      }
    });
  }

  public String currentConnectionId() {
    return currentConnectionId;
  }

  public void createTopic(String topicName) {
    withCluster(currentKafkaClusterId, () -> {
      createTopic(topicName, 1, (short) 1);
    });
  }

  public void deleteTopic(String topicName) {
    withCluster(currentKafkaClusterId, () -> {
      givenDefault()
          .delete("%s/kafka/v3/clusters/{cluster_id}/topics/%s".formatted(sidecarHost, topicName))
          .then()
          .statusCode(204);
    });
  }

  public void deleteAllTopics(String clusterId) {
    Log.debugf("Deleting all topics from cluster %s", clusterId);
    if (currentConnectionId != null) {
      setCurrentCluster(clusterId);
      var topics = listTopics();
      for (var topic : topics) {
        if (!topic.startsWith("_")) {
          deleteTopic(topic);
        } else {
          Log.debugf("Skipping deletion of internal topic %s", topic);
        }
      }
    }
  }

  public Set<String> listTopics() {
    return fromCluster(currentKafkaClusterId, () -> {
      List<Map<String, String>> topics = givenDefault()
          .get("/kafka/v3/clusters/{cluster_id}/topics")
          .then()
          .statusCode(200)
          .extract().body().jsonPath().getList("data");
      return topics
          .stream()
          .filter(not(t -> t.get("topic_name").startsWith("_"))) // ignore internal topics
          .map(t -> t.get("topic_name")).collect(Collectors.toSet());
      });
  }

  public Set<String> listSubjects(String srClusterId) {
    return fromCluster(currentSchemaClusterId, () -> {
      List<String> subjects = givenConnectionId()
          .header("X-cluster-id", srClusterId)
          .get("/subjects")
          .then()
          .statusCode(200)
          .extract().body().jsonPath().getList(".");
      return new HashSet<>(subjects);
    });
  }

  public void deleteAllSubjects(String srClusterId) {
    Log.debugf("Deleting all subjects from cluster %s", srClusterId);
    if (currentConnectionId != null) {
      var subjects = listSubjects(srClusterId);
      for (var subject : subjects) {
        deleteSubject(subject, srClusterId);
      }
    }
  }

  /**
   * Deletes all referenced subjects recursively before deleting the subject itself.
   */
  public void deleteSubject(String subject, String srClusterId) {
    // Get referenced by schema ids
    var resp = givenConnectionId()
        .header("X-cluster-id", srClusterId)
        .get("/subjects/%s/versions/latest/referencedby".formatted(subject))
        .then()
        .extract().response();

    // If 404, ignore
    if (resp.statusCode() == 404) {
      return;
    }
    assertEquals(200, resp.statusCode(), resp.body().asString());
    List<Integer> schemaIds = resp.jsonPath().getList(".", Integer.class);

    // Delete all versions
    // Use GET /schemas/ids/{int: id}/versions
    // Returns subject (string) – Name of the subject
    //         version (int) – Version of the returned schema
    for (var schemaId : schemaIds) {
      var respSchema = givenConnectionId()
          .header("X-cluster-id", srClusterId)
          .get("/schemas/ids/%d/versions".formatted(schemaId))
          .then()
          .extract().response();
      assertEquals(200, resp.statusCode(), resp.body().asString());
      var versions = respSchema.jsonPath().getList(".", SchemaReference.class);

      for (var version : versions) {
        // Recursively delete the subject
        deleteSubject(version.getSubject(), srClusterId);
      }
    }

    // Soft delete
    var deleteResp = givenConnectionId()
        .header("X-cluster-id", srClusterId)
        .delete("/subjects/%s".formatted(subject))
        .then()
        .extract().response();

    assertEquals(200, deleteResp.statusCode(), resp.body().asString());
  }

  public void deleteAllContent() {
    usedSchemaRegistries.forEach(sr -> {
      useConnection(sr.connectionId());
      deleteAllSubjects(sr.id());
    });
    usedKafkaClusters.forEach(kafka -> {
      useConnection(kafka.connectionId());
      deleteAllTopics(kafka.id());
    });
  }

  public void withCluster(String clusterId, Runnable action) {
    var oldClusterId = this.currentClusterId;
    try {
      setCurrentCluster(clusterId);
      action.run();
    } finally {
      this.currentClusterId = oldClusterId;
    }
  }

  public <T> T fromCluster(String clusterId, Supplier<T> action) {
    var oldClusterId = this.currentClusterId;
    try {
      setCurrentCluster(clusterId);
      return action.get();
    } finally {
      this.currentClusterId = oldClusterId;
    }
  }

  public void useClusters(KafkaCluster kafkaCluster, SchemaRegistry schemaRegistry) {
    if (kafkaCluster != null) {
      currentKafkaClusterId = kafkaCluster.id();
      usedKafkaClusters.add(kafkaCluster);
    } else {
      currentKafkaClusterId = null;
    }

    if (schemaRegistry != null) {
      currentSchemaClusterId = schemaRegistry.id();
      usedSchemaRegistries.add(schemaRegistry);
    } else {
      currentSchemaClusterId = null;
    }
  }

  private String generateConnectionId() {
    return CONNECTION_ID_TEMPLATE.formatted(CONNECTION_COUNTER.incrementAndGet());
  }

  public void forEachConnection(ConnectionType type, Consumer<Connection> action) {
    forEachConnection(c -> c.spec().type() == type, action);
  }

  public void forEachConnection(Consumer<Connection> action) {
    forEachConnection(c -> true, action);
  }

  public void forEachConnection(Predicate<Connection> filter, Consumer<Connection> action) {
    var oldCurrentConnectionId = this.currentConnectionId;
    try {
      listConnections()
          .stream()
          .filter(filter)
          .forEach(connection -> {
            this.currentConnectionId = connection.id();
            action.accept(connection);
          });
    } finally {
      this.currentConnectionId = oldCurrentConnectionId;
    }
  }

  public List<Connection> listConnections() {
    var response = given()
        .when()
        .get("%s/gateway/v1/connections".formatted(sidecarHost))
        .then();
    response.statusCode(200);
    var list = response.extract().response().body().as(ConnectionsList.class);
    return list.data();
  }

  public Connection createLocalConnection(String schemaRegistryUri) {
    var connectionId = generateConnectionId();
    LocalConfig localConfig = null;
    if (schemaRegistryUri != null) {
      localConfig = new LocalConfig(schemaRegistryUri);
    }
    return createConnection(
        ConnectionSpec.createLocal(
            connectionId,
            connectionId,
            localConfig
        )
    );
  }

  public void useConnection(String connectionId) {
    currentConnectionId = connectionId;
  }

  public void setCurrentCluster(String clusterId) {
    currentClusterId = clusterId;
  }

  @Override
  public ValidatableResponse testConnectionWithResponse(ConnectionSpec spec) {
    return given()
        .contentType(ContentType.JSON)
        .body(spec)
        .queryParam("dry_run", true)
        .post("%s/gateway/v1/connections".formatted(sidecarHost))
        .then();
  }

  @Override
  public Connection testConnection(ConnectionSpec spec) {
    var response = given()
        .contentType(ContentType.JSON)
        .body(spec)
        .queryParam("dry_run", true)
        .post("%s/gateway/v1/connections".formatted(sidecarHost))
        .then()
        .statusCode(200);
    var connection = response.extract().response().body().as(Connection.class);
    assertEquals(spec.id(), connection.id());
    // Do not use this connection for subsequent operations
    return connection;
  }

  @Override
  public Connection createConnection(ConnectionSpec spec) {
    // Create connection
    try {
      given()
          .contentType(ContentType.JSON)
          .body(OBJECT_MAPPER.writeValueAsString(spec))
          .post("%s/gateway/v1/connections".formatted(sidecarHost))
          .then()
          .statusCode(200)
          .body("spec.id", equalTo(spec.id()));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    // If the connection spec configures a Kafka cluster or a Schema Registry, wait until the
    // connection to the Kafka cluster or Schema registry has been established
    if (spec.kafkaClusterConfig() != null || spec.schemaRegistryConfig() != null) {
      await().atMost(Duration.ofSeconds(10)).until(() -> {
        var connection = given()
            .when()
            .get("%s/gateway/v1/connections/%s".formatted(sidecarHost, spec.id()))
            .then()
            .statusCode(200)
            .extract().response().body().as(Connection.class);
        var kafkaClusterIsNullOrConnected = spec.kafkaClusterConfig() == null
            || connection.status().kafkaCluster().isConnected();
        var schemaRegistryIsNullOrConnected = spec.schemaRegistryConfig() == null
            || connection.status().schemaRegistry().isConnected();
        return kafkaClusterIsNullOrConnected && schemaRegistryIsNullOrConnected;
      });
    }

    // Get and use this connection for subsequent operations
    var connection = given()
        .when()
        .get("%s/gateway/v1/connections/%s".formatted(sidecarHost, spec.id()))
        .then()
        .statusCode(200)
        .extract().response().body().as(Connection.class);
    currentConnectionId = connection.id();
    return connection;
  }

  public Connection createLocalConnectionTo(TestEnvironment env, Class<?> scope) {
    return createLocalConnectionTo(env, scope.getName());
  }

  public Connection createLocalConnectionTo(TestEnvironment env, String scope) {
    var spec = env.localConnectionSpec().orElseThrow();
    // Append the scope to the name of the connection
    spec = spec.withName( "%s (%s)".formatted(spec.name(), scope));
    spec = spec.withId( "%s-%s".formatted(spec.id(), scope));
    return createConnection(spec);
  }

  public Connection createDirectConnectionTo(TestEnvironment env, Class<?> scope) {
    return createDirectConnectionTo(env, scope.getName());
  }

  public Connection createDirectConnectionTo(TestEnvironment env, String scope) {
    var spec = env.directConnectionSpec().orElseThrow();
    // Append the scope to the name of the connection
    spec = spec.withName( "%s (%s)".formatted(spec.name(), scope));
    spec = spec.withId( "%s-%s".formatted(spec.id(), scope));
    return createConnection(spec);
  }

  public void deleteConnection(String connectionId) {
    given()
        .when()
        .delete("%s/gateway/v1/connections/%s".formatted(sidecarHost, connectionId))
        .then()
        .statusCode(204);
    if (connectionId.equals(currentConnectionId)) {
      currentConnectionId = null;
    }
  }

  public void deleteAllConnections() {
    deleteAllConnections(c -> true);
  }

  public void deleteAllConnections(Predicate<Connection> filter) {
    var connections = listConnections();
    for (var connection : connections) {
      if (filter == null || filter.test(connection)) {
        deleteConnection(connection.id());
      }
    }
  }

  public RequestSpecification givenDefault() {
    return givenConnectionId()
        .when()
        .header("Content-Type", "application/json")
        .pathParams(clusterIdPathParams());
  }

  public RequestSpecification givenConnectionId() {
    return givenConnectionId(currentConnectionId);
  }

  public RequestSpecification givenConnectionId(String connectionId) {
    return given()
        .config(
            // https://stackoverflow.com/a/67876342 saves the day
            // Not specifying this will lead to a `java.util.zip.ZipException: Not in GZIP format` error
            // even though the response is not gzipped, because RestAssured tries to decode it as such
            // by default. This is a workaround to disable the default decoders.
            RestAssured.config().decoderConfig(DecoderConfig.decoderConfig().noContentDecoders())
        )
        .header("X-connection-id", connectionId);
  }

  public Map<String, String> clusterIdPathParams() {
    assertNotNull(currentClusterId, "Current cluster ID is not set");
    return Map.of("cluster_id", currentClusterId);
  }

  public void shouldRaiseErrorWhenConnectionIdIsMissing(String path) {
    given()
        .when()
        .get(path)
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Missing required header: x-connection-id"));
  }

  public void shouldRaiseErrorWhenConnectionNotFound(String path) {
    givenConnectionId("non-existent-connection")
        .when()
        .get(path)
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Connection not found: non-existent-connection"));
  }

  public void produceRecord(
      Integer partitionId,
      String topicName,
      Object key,
      Object value
  ) {
    produceRecord(partitionId, topicName, key, null, value, null);
  }

  public void produceRecord(
      Integer partitionId,
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  ) {
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var resp = produceRecordThen(
              partitionId, topicName, key, keySchemaVersion, value, valueSchemaVersion
          ).extract().response();
          // Log the response body in case of an error to ease debugging
          assertEquals(200, resp.statusCode(), resp.body().asString());
        });
  }

  public void produceRecord(
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  ) {
    produceRecord(null, topicName, key, keySchemaVersion, value, valueSchemaVersion);
  }

  public ValidatableResponse produceRecordThen(
      Integer partitionId,
      String topicName,
      Object key,
      Object value
  ) {
    return produceRecordThen(partitionId, topicName, key, null, value, null);
  }

  public ValidatableResponse produceRecordThen(
      Integer partitionId,
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  ) {
    return fromCluster(currentKafkaClusterId, () ->
        givenDefault()
            .body(createProduceRequest(partitionId, key, keySchemaVersion, value, valueSchemaVersion))
            .post("/kafka/v3/clusters/{cluster_id}/topics/%s/records".formatted(topicName))
            .then()
    );
  }

  public ValidatableResponse produceRecordThen(
      String topicName,
      ProduceRequest request
  ) {
    return produceRecordThen(topicName, request, false);
  }

  public ValidatableResponse produceRecordThen(
      String topicName,
      ProduceRequest request,
      Boolean dryRun
  ) {
    return fromCluster(currentKafkaClusterId, () ->
        givenDefault()
            .body(request)
            .queryParam("dry_run", dryRun)
            .post("/kafka/v3/clusters/{cluster_id}/topics/%s/records".formatted(topicName))
            .then()
    );
  }

  public ProduceRequest createProduceRequest(
      Integer partitionId,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  ) {
    return ProduceRequest
        .builder()
        .partitionId(partitionId)
        .key(
            ProduceRequestData
                .builder()
                .schemaVersion(keySchemaVersion)
                .data(key)
                .build()
        )
        .value(
            ProduceRequestData
                .builder()
                .schemaVersion(valueSchemaVersion)
                .data(value)
                .build()
        )
        .build();
  }

  public SimpleConsumeMultiPartitionResponse consume(
      String topicName, SimpleConsumeMultiPartitionRequest requestBody
  ) {
    var response = await()
        .atMost(Duration.ofSeconds(10))
        .until(() ->
                fromCluster(currentKafkaClusterId, () ->
                    givenDefault()
                        .body(requestBody)
                        .post("/gateway/v1/clusters/{cluster_id}/topics/%s/partitions/-/consume"
                            .formatted(topicName)
                        )
                        .then()
                ),
            resp -> resp.extract().statusCode() == 200
        );
    return response
        .extract()
        .body()
        .as(SimpleConsumeMultiPartitionResponse.class);
  }

  /**
   * Produce plain old String key/value records to a topic
   */
  public void produceStringRecords(String topicName, String[][] records) {
    for (var record: records) {
      produceRecord(
          topicName,
          record[0],
          null,
          record[1],
          null
      );
    }
  }

  public Schema createSchema(
      String subject, String schemaType, String schema, List<SchemaReference> references
  ) {
    return fromCluster(currentSchemaClusterId, () -> {
          var versionRequest = new HashMap<String, Object>(Map.of(
              "schemaType", schemaType,
              "schema", schema
          ));

          if (references != null) {
            versionRequest.put("references", references);
          }

          var createSchemaVersionResp = givenConnectionId()
            .headers(
                "Content-Type", "application/json",
                "X-cluster-id", currentSchemaClusterId
            )
            .body(versionRequest)
            .post("/subjects/%s/versions".formatted(subject))
            .then().extract().response();

        if (createSchemaVersionResp.statusCode() != 200) {
          fail("Failed to create schema: %s".formatted(createSchemaVersionResp.body().asString()));
        } else {
          assertEquals(200, createSchemaVersionResp.statusCode());
        }

      await()
           .pollDelay(Duration.ofMillis(20))
           .pollInterval(Duration.ofMillis(10))
           .atMost(Duration.ofMillis(250))
           .until(()->getLatestSchemaVersion(subject, currentSchemaClusterId) != null);

        return getLatestSchemaVersion(subject, currentSchemaClusterId);
    });
  }

  public Schema createSchema(String subject, String schemaType, String schema) {
      return createSchema(subject, schemaType, schema, null);
  }

  public Schema getLatestSchemaVersion(String subject, String srClusterId) {
    return givenConnectionId()
        .headers("X-cluster-id", srClusterId)
        .get("/subjects/%s/versions/latest".formatted(subject))
        .then()
        .statusCode(200)
        .contentType("application/vnd.schemaregistry.v1+json")
        .extract()
        .body()
        .as(Schema.class);
  }

  public Optional<SchemaRegistry> getSchemaRegistryCluster() {
    return getSchemaRegistryCluster("localConnections", currentConnectionId).or(
        () -> getSchemaRegistryCluster("directConnections", currentConnectionId)
    );
  }

  public Optional<SchemaRegistry> getSchemaRegistryCluster(String query, String connectionId) {
    var queryLocalConnections = """
        { "query": "query %s {
            %s{
              id,
              schemaRegistry {
                id
                uri
              }
            }
          }"
        }
        """.formatted(query, query);
    var graphQlResponse = given()
        .when()
        .header("Content-Type", "application/json")
        .body(queryLocalConnections)
        .post("/gateway/v1/graphql")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    Log.debug("GraphQL response: %s".formatted(graphQlResponse));

    var results = asJson(graphQlResponse)
        .get("data")
        .get(query);
    if (results == null || results.isEmpty()) {
      return Optional.empty();
    }
    var localConnections = results.elements();
    // Find the connection
    while (localConnections.hasNext()) {
      var connection = localConnections.next();
      if (connection.has("id")
          && connection.get("id").asText().equals(connectionId)
          && !connection.get("schemaRegistry").isNull()) {
        var clusterId = connection.get("schemaRegistry").get("id").asText();
        var clusterUri = connection.get("schemaRegistry").get("uri").asText();
        return Optional.of(
            new SchemaRegistry(connectionId, clusterId, clusterUri)
        );
      }
    }
    return Optional.empty();
  }

  public Optional<KafkaCluster> getKafkaCluster() {
    return getKafkaCluster("localConnections", currentConnectionId).or(
        () -> getKafkaCluster("directConnections", currentConnectionId)
    );
  }

  public Optional<KafkaCluster> getKafkaCluster(String query, String connectionId) {
    var queryLocalConnections = """
        { "query": "query %s {
            %s{
              id,
              kafkaCluster {
                id,
                bootstrapServers
              }
            }
          }"
        }
        """.formatted(query, query);
    var graphQlResponse = given()
        .when()
        .header("Content-Type", "application/json")
        .body(queryLocalConnections)
        .post("/gateway/v1/graphql")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    Log.debug("GraphQL response: %s".formatted(graphQlResponse));

    var results = asJson(graphQlResponse)
        .get("data")
        .get(query);
    if (results == null) {
      return Optional.empty();
    }
    var localConnections = results.elements();
    // Find the connection
    while (localConnections.hasNext()) {
      var connection = localConnections.next();
      if (connection.has("id") && connection.get("id").asText().equals(connectionId)) {
        var clusterId = connection.get("kafkaCluster").get("id").asText();
        var bootstrapServers = connection.get("kafkaCluster").get("bootstrapServers").asText();
        return Optional.of(
            new KafkaCluster(connectionId, clusterId, bootstrapServers)
        );
      }
    }
    return Optional.empty();
  }

  public String randomTopicName() {
    return "test-topic-" + UUID.randomUUID();
  }

  public String loadCCloudConnectionsGraphQL() {
    return loadResource("graph/real/all-ccloud-connections-query.graphql");
  }

  public String loadDirectConnectionsGraphQL() {
    return loadResource("graph/real/direct-connections-query.graphql");
  }

  public String loadLocalConnectionsGraphQL() {
    return loadResource("graph/real/local-connections-query.graphql");
  }

  public ValidatableResponse submitGraphQL(String query) {
    return queryGraphQLRaw(query);
  }

  public ValidatableResponse submitLocalConnectionsGraphQL() {
    return submitGraphQL(
        loadLocalConnectionsGraphQL()
    );
  }

  public ValidatableResponse submitDirectConnectionsGraphQL() {
    return submitGraphQL(
        loadDirectConnectionsGraphQL()
    );
  }

  public ValidatableResponse submitCCloudConnectionsGraphQL() {
    return submitGraphQL(
        loadCCloudConnectionsGraphQL()
    );
  }

  public boolean localConnectionsGraphQLResponseContains(String connectionId) {
    return submitLocalConnectionsGraphQL()
        .extract()
        .response()
        .jsonPath()
        .getList("data.localConnections", Map.class)
        .stream()
        .anyMatch(m -> m.get("id").equals(connectionId));
  }

  public boolean directConnectionsGraphQLResponseContains(String connectionId) {
    return submitDirectConnectionsGraphQL()
        .extract()
        .response()
        .jsonPath()
        .getList("data.directConnections", Map.class)
        .stream()
        .anyMatch(m -> m.get("id").equals(connectionId));
  }


  public static class PasswordSerializer extends RawSerializer<Password> {

  }

  /**
   * This serializer is unsafe to use in production code, as it will serialize the raw value of the
   * {@link Redactable} object. It is intended for use in tests only.
   */
  protected abstract static class RawSerializer<T extends Redactable> extends JsonSerializer<T> {

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers) {
      try {
        var chars = value.asCharArray();
        gen.writeString(chars, 0, chars.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
