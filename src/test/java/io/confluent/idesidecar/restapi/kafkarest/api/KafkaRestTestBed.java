package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.restassured.RestAssured.given;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.confluent.idesidecar.restapi.util.KafkaTestBed;
import io.restassured.http.ContentType;
import java.util.Properties;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
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
}
