package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import java.util.*;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.stream.Collectors;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;


@TestProfile(NoAccessFilterProfile.class)
public class ConfluentLocalTestBed implements AutoCloseable {
  private static final String KAFKA_INTERNAL_LISTENER = "PLAINTEXT://confluent-local-broker-1:29092";
  private static Network network;
  private static ConfluentLocalKafkaWithRestProxyContainer confluent;
  private static SchemaRegistryContainer schemaRegistry;
  protected static final String CONNECTION_ID = "test-connection";

  public static final Integer TEST_PORT = ConfigProvider.getConfig()
      .getValue("quarkus.http.test-port", Integer.class);

  private static final String sidecarHost = "http://localhost:%s".formatted(TEST_PORT);

  @BeforeAll
  public static void start() {
    initialize();

    confluent.start();
    schemaRegistry.start();
  }

  private static void initialize() {
    network = Network.newNetwork();
    confluent = new ConfluentLocalKafkaWithRestProxyContainer()
        .withNetwork(network)
        .withNetworkAliases("kafka")
        .waitingFor(Wait.forLogMessage(
            ".*Server started, listening for requests.*\\n", 1))
        // Kafka REST server port
        .waitingFor(Wait.forListeningPorts(
            ConfluentLocalKafkaWithRestProxyContainer.REST_PROXY_PORT
        ));

    schemaRegistry = new SchemaRegistryContainer(KAFKA_INTERNAL_LISTENER)
        .withNetwork(network)
        .withExposedPorts(8081)
        .withNetworkAliases("schema-registry")
        .dependsOn(confluent)
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
  }

  @AfterAll
  public static void stop() {
    confluent.stop();
    schemaRegistry.stop();
  }

  @BeforeEach
  public void beforeEach() {
    createConnection();
  }

  @AfterEach
  public void afterEach() {
    deleteAllSubjects();
    deleteAllTopics();
    deleteConnection();
  }

  @Override
  public void close() {
    schemaRegistry.close();
    confluent.close();
    network.close();
  }

  public static void createTopic(String topicName, int partitions, short replicationFactor) {
    givenDefault()
        .body(
            CreateTopicRequestData
                .builder()
                .topicName(topicName)
                .partitionsCount(partitions)
                .replicationFactor((int) replicationFactor)
                .build()
        )
        .post("/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(200);
  }

  public static void createTopic(String topicName) {
    createTopic(topicName, 1, (short) 1);
  }

  public void deleteTopic(String topicName) {
    givenDefault()
        .delete("%s/kafka/v3/clusters/{cluster_id}/topics/%s".formatted(sidecarHost, topicName))
        .then()
        .statusCode(204);
  }

  private void deleteAllTopics() {
    var topics = listTopics();
    for (var topic : topics) {
      deleteTopic(topic);
    }
  }

  public static Set<String> listTopics() {
    List<Map<String, String>> topics = givenDefault()
        .get("/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(200)
        .extract().body().jsonPath().getList("data");
    return topics
        .stream()
        .filter(t -> !t.get("topic_name").startsWith("_"))
        .map(t -> t.get("topic_name")).collect(Collectors.toSet());
  }

  public static Set<String> listSubjects(String srClusterId) {
    List<String> subjects = givenConnectionId()
        .header("X-cluster-id", srClusterId)
        .get("/subjects")
        .then()
        .statusCode(200)
        .extract().body().jsonPath().getList(".");
    return new HashSet<>(subjects);
  }

  public static void deleteAllSubjects() {
    var schemaRegistry = getSchemaRegistryCluster();
    var subjects = listSubjects(schemaRegistry.id());
    for (var subject : subjects) {
      deleteSubject(subject, schemaRegistry.id());
    }
  }

  private static void deleteSubject(String subject, String srClusterId) {

    // Soft delete
    givenConnectionId()
        .header("X-cluster-id", srClusterId)
        .delete("/subjects/%s".formatted(subject))
        .then()
        .statusCode(200);
  }

  private static void createConnection() {
    given()
        .contentType(ContentType.JSON)
        .body(new ConnectionSpec(
            CONNECTION_ID,
            CONNECTION_ID,
            ConnectionSpec.ConnectionType.LOCAL,
            null,
            null
        ))
        .when()
        .post("%s/gateway/v1/connections".formatted(sidecarHost))
        .then()
        .statusCode(200);
  }

  private static void deleteConnection() {
    given()
        .when()
        .delete("%s/gateway/v1/connections/%s".formatted(sidecarHost, CONNECTION_ID))
        .then()
        .statusCode(204);
  }

  protected static RequestSpecification givenDefault() {
    return givenConnectionId()
        .when()
        .header("Content-Type", "application/json")
        .pathParams(clusterIdPathParams());
  }

  protected static RequestSpecification givenConnectionId() {
    return givenConnectionId(CONNECTION_ID);
  }

  protected static RequestSpecification givenConnectionId(String connectionId) {
    return given()
        .header("X-connection-id", connectionId);
  }

  protected static Map<String, String> clusterIdPathParams() {
    return Map.of("cluster_id", ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID);
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

  public static void produceRecord(
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  ) {
    givenDefault()
        .body(
            ProduceRequest.builder()
                .key(
                    ProduceRequestData.builder()
                        .schemaVersion(keySchemaVersion)
                        .data(key)
                        .build()
                )
                .value(
                    ProduceRequestData.builder()
                        .schemaVersion(valueSchemaVersion)
                        .data(value)
                        .build()
                )
                .build()
        )
        .post("/kafka/v3/clusters/{cluster_id}/topics/%s/records".formatted(topicName))
        .then()
        .statusCode(200);
  }

  /**
   * Produce plain old String key/value records to a topic
   */
  public static void produceStringRecords(String topicName, String[][] records) {
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

  public Integer createSchema(String subject, String schemaType, String schema) {
    var srCluster = getSchemaRegistryCluster();
    givenConnectionId()
        .headers(
            "Content-Type", "application/vnd.schemaregistry.v1+json",
            "X-cluster-id", srCluster.id()
        )
        .body(Map.of(
            "schemaType", schemaType,
            "schema", schema
        ))
        .post("/subjects/%s/versions".formatted(subject))
        .then()
        .statusCode(200);

    // Sleep for a bit to allow the schema to be registered
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      // Ignore
    }

    return getLatestSchemaVersion(subject, srCluster.id());
  }

  private Integer getLatestSchemaVersion(String subject, String srClusterId) {
    var response = givenConnectionId()
        .headers("X-cluster-id", srClusterId)
        .get("/subjects/%s/versions/latest".formatted(subject))
        .then()
        .statusCode(200)
        .contentType("application/vnd.schemaregistry.v1+json")
        .extract()
        .body()
        .asString();
    return Objects.requireNonNull(asJson(response)).get("version").asInt();
  }

  public record SchemaRegistry(String id, String uri) {

  }

  public static SchemaRegistry getSchemaRegistryCluster() {
    var queryLocalConnections = """
        { "query": "query localConnections {
            localConnections{
              schemaRegistry {
                id
                uri
              }
            }
          }"
        }
        """;
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

    var localConnections = asJson(graphQlResponse)
        .get("data")
        .get("localConnections")
        .elements();
    assertTrue(localConnections.hasNext(), "Could not find local connections");
    var localConnection = localConnections.next();
    return new SchemaRegistry(
        localConnection.get("schemaRegistry").get("id").asText(),
        localConnection.get("schemaRegistry").get("uri").asText()
    );
  }

  public static String getBootstrapServers() {
    return confluent.getKafkaBootstrapServers();
  }
}