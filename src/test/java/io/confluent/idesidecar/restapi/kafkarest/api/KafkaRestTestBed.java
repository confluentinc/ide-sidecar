package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.confluent.idesidecar.restapi.util.KafkaTestBed;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.util.Map;
import java.util.Properties;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.wait.strategy.Wait;

public class KafkaRestTestBed extends KafkaTestBed {
  private static ConfluentLocalKafkaWithRestProxyContainer confluentLocal;
  protected static final String CONNECTION_ID = "test-connection";

  private static final Integer testPort = ConfigProvider.getConfig()
      .getValue("quarkus.http.test-port", Integer.class);

  @BeforeAll
  static void setup() {
    confluentLocal = new ConfluentLocalKafkaWithRestProxyContainer()
        .waitingFor(Wait.forLogMessage(
            ".*Server started, listening for requests.*\\n", 1))
        // Kafka REST server port
        .waitingFor(Wait.forListeningPorts(
            ConfluentLocalKafkaWithRestProxyContainer.REST_PROXY_PORT
        ));
    confluentLocal.start();

    // Create a connection
    KafkaRestTestBed.createConnection();
  }

  @AfterEach
  void cleanup() throws Exception {
    // Delete all topics
    deleteAllTopics();
  }

  private void deleteAllTopics() throws Exception {
    for (var topic : listTopics()) {
      deleteTopic(topic);
    }
    assert listTopics().isEmpty();
  }

  private static void createConnection() {
    given()
        .contentType(ContentType.JSON)
        .body(new ConnectionSpec(
            CONNECTION_ID,
            CONNECTION_ID,
            // Connection type does not matter for this test... yet
            ConnectionType.LOCAL,
            null,
            null
        ))
        .when().post("http://localhost:%s/gateway/v1/connections".formatted(
            testPort))
        .then()
        .statusCode(200);
  }

  private static void deleteConnection() {
    given()
        .when().delete("http://localhost:%s/gateway/v1/connections/%s".formatted(
            testPort, CONNECTION_ID))
        .then()
        .statusCode(204);
  }

  @AfterAll
  static void teardown() {
    confluentLocal.stop();
    deleteConnection();
  }

  @Override
  protected Properties getKafkaProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", confluentLocal.getKafkaBootstrapServers());
    return properties;
  }

  protected static RequestSpecification givenDefault() {
    return givenConnectionId()
        .when()
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

  void shouldRaiseErrorWhenConnectionIdIsMissing(String path) {
    given()
        .when()
        .get(path)
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Missing required header: x-connection-id"));
  }

  void shouldRaiseErrorWhenConnectionNotFound(String path) {
    givenConnectionId("non-existent-connection")
        .when()
        .get(path)
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Connection not found: non-existent-connection"));
  }
}
