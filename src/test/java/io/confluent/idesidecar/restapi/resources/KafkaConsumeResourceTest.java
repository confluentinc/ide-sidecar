package io.confluent.idesidecar.restapi.resources;

import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectKafkaClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectKafkaClusterNotInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectSchemaRegistryForKafkaClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectSchemaRegistryInCache;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.testutil.JsonMatcher;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class KafkaConsumeResourceTest {
  @Inject
  ConnectionStateManager connectionStateManager;

  @InjectMock
  ClusterCache clusterCache;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;
  CCloudTestUtil ccloudTestUtil;
  private static final String KAFKA_CLUSTER_ID = "lkc-abcd123";
  private static final String SCHEMA_REGISTRY_CLUSTER_ID = "lsrc-defg456";
  private static final String CONNECTION_ID = "fake-connection-id";
  private static final Map<String, String> CLUSTER_REQUEST_HEADERS = Map.of(
      "x-connection-id", CONNECTION_ID,
      "x-cluster-id", KAFKA_CLUSTER_ID
  );
  private static final String SCHEMA_LESS_TOPIC_NAME = "test-topic";
  private static final String AVRO_SCHEMA_TOPIC_NAME = "orders_avro";
  private static final String PROTOBUF_SCHEMA_TOPIC_NAME = "orders_proto";
  // This value is encoded in the __raw__ field of the records returned from consuming the
  // AVRO_SCHEMA_TOPIC_NAME topic
  private static final String ORDERS_AVRO_SCHEMA_ID = "100002";
  private static final String ORDERS_PROTOBUF_SCHEMA_ID = "100003";
  private static final String CCLOUD_SIMPLE_CONSUME_API_PATH =
      "/kafka/v3/clusters/%s/internal/topics/%s/partitions/-/records:consume_guarantee_progress";

  private static final String SCHEMAS_BY_ID_URL_REGEX = "/schemas/ids/%s.*";

  @BeforeEach
  void setUp() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
  }

  @AfterEach
  void tearDown() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.resetMappings();
    clusterCache.clear();
  }

  @Test
  void testConnectionHeaderNotPassedReturns400() {
    final String path = "/gateway/v1/clusters/%s/topics/topic_3/partitions/-/consume"
        .formatted(KAFKA_CLUSTER_ID);

    given()
        .when()
        .contentType(ContentType.JSON)
        .body("{}")
        .post(path)
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("x-connection-id header is required"));
  }

  @Test
  void testConnectionNotFoundReturns404() {
    final String path = "/gateway/v1/clusters/%s/topics/topic_3/partitions/-/consume"
        .formatted(KAFKA_CLUSTER_ID);
    given()
        .when()
        .contentType(ContentType.JSON)
        .header("x-connection-id", CONNECTION_ID)
        .body("{}")
        .post(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Connection id=%s not found".formatted(CONNECTION_ID)));
  }

  @Test
  void testClusterIdHeaderNotPassedReturns400() {
    final String path = "/gateway/v1/clusters/%s/topics/topic_3/partitions/-/consume"
        .formatted(KAFKA_CLUSTER_ID);
    // Given we have an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    given()
        .when()
        .contentType(ContentType.JSON)
        .header("x-connection-id", CONNECTION_ID)
        .header("x-cluster-id", "Foo")
        .body("{}")
        .post(path)
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("Cluster ID in path and header do not match"));
  }

  @Test
  void testNonExistentClusterInfoReturns404() {
    final String path = "/gateway/v1/clusters/%s/topics/topic_3/partitions/-/consume"
        .formatted(KAFKA_CLUSTER_ID);
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    expectKafkaClusterNotInCache(clusterCache, CONNECTION_ID, KAFKA_CLUSTER_ID);

    // Now trying to hit the cluster proxy endpoint without cached cluster info
    // should return a 500 error
    given()
        .when()
        .contentType(ContentType.JSON)
        .header("x-connection-id", CONNECTION_ID)
        .body("{}")
        .header("x-cluster-id", KAFKA_CLUSTER_ID)
        .post(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString(
            "not found in connection %s".formatted(CONNECTION_ID)
        ));
  }


  @Test
  void testConsumePropagatesNon200StatusFromCCloud() {
    setupSimpleConsumeApi();
    wireMock.register(
        WireMock
            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
                KAFKA_CLUSTER_ID, "topic_429"))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(429)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-connection-id", CONNECTION_ID)
                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
                    .withBody("Too many requests.")
            )
            .atPriority(100));
    final String path = "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
        .formatted(KAFKA_CLUSTER_ID, "topic_429");
    // Now trying to hit the cluster proxy endpoint
    // should return a 429 Too many requests error.
    given()
        .when()
        .contentType(ContentType.JSON)
        .header("x-connection-id", CONNECTION_ID)
        .body("{}")
        .header("x-cluster-id", KAFKA_CLUSTER_ID)
        .post(path)
        .then()
        .statusCode(429)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString(
            "Error fetching the messages from ccloud"))
        .body("title", containsString("Too many requests"));
  }

  @Test
  void testCCloudReturnsErrorReturns500() {
    setupSimpleConsumeApi();
    var errorMessage = "{\"error\": \"Something went wrong.\"}";
    wireMock.register(
        WireMock
            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
                KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .withRequestBody(equalToJson("{}"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(500)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-connection-id", CONNECTION_ID)
                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
                    .withBody(errorMessage)
            )
            .atPriority(100)
    );

    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then()
        .statusCode(500)
        .body("title", containsString("{\"error\": \"Something went wrong.\"}"));
  }

  @Test
  void testConsumeRecordsAgainstCCloud() {
    setupSimpleConsumeApi();
    var actualResponse = given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);

    var actualResponseBody = actualResponse.extract().asString();
    var expectedResponseBody = loadResource("message-viewer/ccloud-message-sidecar-response.json");
    JsonObject expectedJson = new JsonObject(expectedResponseBody);
    JsonObject actualJson = new JsonObject(actualResponseBody);

    assertEquals(expectedJson, actualJson);

    // Endpoint should return the number of consumed bytes in the response headers
    actualResponse.header(
        KafkaConsumeResource.KAFKA_CONSUMED_BYTES_RESPONSE_HEADER,
        String.valueOf(expectedJson.toString().length()));
  }

  @Test
  void testConsumeRecordsAvroSchemaTopic() throws JsonProcessingException {
    var expectedAvroResponse = loadResource("message-viewer/consume-avro-topic-expected-response.json");
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode expectedNode = objectMapper.readTree(expectedAvroResponse);

    setupSimpleConsumeApi();
    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, AVRO_SCHEMA_TOPIC_NAME)
        )
        .then()
        .statusCode(200)
        .header(
            KafkaConsumeResource.KAFKA_CONSUMED_BYTES_RESPONSE_HEADER,
            String.valueOf(expectedNode.toString().length())
        )
        .body(JsonMatcher.matchesJson(expectedAvroResponse));

    // It's not only enough to check for 200 since we simply fall back to returning
    // raw bytes if we fail to deserialize the message.

    // Observation: We seem to make two requests to the sidecar SR proxy.
    //              One without the subject= query param and one with it.
    //              This is left as an exercise for the reader to investigate.
    wireMock.verifyThat(
        exactly(2),
        getRequestedFor(urlMatching(SCHEMAS_BY_ID_URL_REGEX.formatted(ORDERS_AVRO_SCHEMA_ID)))
    );
  }

  @Test
  void testConsumeRecordsProtoSchemaTopic() throws JsonProcessingException {
    var expectedProtoResponse = loadResource("message-viewer/consume-proto-topic-expected-response.json");
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode expectedNode = objectMapper.readTree(expectedProtoResponse);

    setupSimpleConsumeApi();
    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, PROTOBUF_SCHEMA_TOPIC_NAME)
        )
        .then()
        .statusCode(200)
        .body(JsonMatcher.matchesJson(expectedProtoResponse))
        .header(
            KafkaConsumeResource.KAFKA_CONSUMED_BYTES_RESPONSE_HEADER,
            String.valueOf(expectedNode.toString().length())
        );

    // It's not only enough to check for 200 since we simply fall back to returning
    // raw bytes if we fail to deserialize the message.

    // Observation: We seem to make two requests to the sidecar SR proxy.
    //              One without the subject= query param and one with it.
    //              This is left as an exercise for the reader to investigate.
    wireMock.verifyThat(
        exactly(1),
        getRequestedFor(urlMatching(SCHEMAS_BY_ID_URL_REGEX.formatted(ORDERS_PROTOBUF_SCHEMA_ID)))
    );
  }


  /**
   * If we don't get sent a request body, we should still be able to consume records. We check
   * for this and send "{}" as the request body to CCloud.
   */
  @Test
  void testConsumeRecordsWithEmptyRequestBody() {
    setupSimpleConsumeApi();
    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then()
        .statusCode(200);
  }

  @Test
  void testConsumeRecordsWithNonEmptyRequestBody() {
    setupSimpleConsumeApi();

    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{\"from_beginning\": true,\"max_poll_records\":5}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then()
        .statusCode(200);

    wireMock.verifyThat(
        exactly(1),
        postRequestedFor(urlEqualTo(
            CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME))
        ).withRequestBody(equalToJson("{\"from_beginning\": true,\"max_poll_records\":5}"))
    );
  }

  @Test
  void testBadConsumeRequestThrows400() {
    setupSimpleConsumeApi();
    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{\"faucets\": \"on\"}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then()
        .statusCode(400)
        .body("attributeName", is("faucets"));
  }

  @Disabled  // Find a way to inject mock consumer for local kafka cluster
  @Test
  void testConsumeNoWorkyForConfluentLocal() {
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.LOCAL);
    expectKafkaClusterInCache(
        clusterCache,
        CONNECTION_ID,
        KAFKA_CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort)
    );

    given()
        .when()
        .contentType(ContentType.JSON)
        .headers(CLUSTER_REQUEST_HEADERS)
        .body("{\"from_beginning\": true,\"max_poll_records\":5}")
        .post(
            "/gateway/v1/clusters/%s/topics/%s/partitions/-/consume"
                .formatted(KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME)
        )
        .then()
        .statusCode(200)
        .body("title", is("This endpoint does not yet support connection-type=LOCAL"));
  }

  private String getDataPlaneToken() {
    return ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
        .getOauthContext()
        .getDataPlaneToken()
        .token();
  }

  private void setupSimpleConsumeApi() {
    // Create connection and cache its cluster
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // Expect a Kafka cluster exists
    var mockKafkaCluster = expectKafkaClusterInCache(
        clusterCache,
        CONNECTION_ID,
        KAFKA_CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort)
    );
    // Expect a Schema Registry cluster exists by cluster type
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        SCHEMA_REGISTRY_CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        ClusterType.SCHEMA_REGISTRY
    );
    // Expect a Schema Registry cluster exists
    expectSchemaRegistryInCache(
        clusterCache,
        CONNECTION_ID,
        SCHEMA_REGISTRY_CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort)
    );
    // Expect a Schema Registry for the Kafka cluster exists
    expectSchemaRegistryForKafkaClusterInCache(
        clusterCache,
        CONNECTION_ID,
        mockKafkaCluster,
        SCHEMA_REGISTRY_CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort)
    );

    var schemaLessTopicConsumeCCloudResponse = loadResource("message-viewer/ccloud-message.json");

    wireMock.register(
        WireMock
            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
                KAFKA_CLUSTER_ID, SCHEMA_LESS_TOPIC_NAME))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-connection-id", CONNECTION_ID)
                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
                    .withBody(schemaLessTopicConsumeCCloudResponse)
            )
            .atPriority(100));

    var avroSchemaTopicConsumeCCloudResponse = loadResource(
        "message-viewer/ccloud-records-consume-avro.json");

    wireMock.register(
        WireMock
            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
                KAFKA_CLUSTER_ID, AVRO_SCHEMA_TOPIC_NAME))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-connection-id", CONNECTION_ID)
                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
                    .withBody(avroSchemaTopicConsumeCCloudResponse)
            )
            .atPriority(100));

    wireMock.register(
        WireMock
            .get(urlMatching(SCHEMAS_BY_ID_URL_REGEX.formatted(ORDERS_AVRO_SCHEMA_ID)))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .withHeader("target-sr-cluster", new EqualToPattern(SCHEMA_REGISTRY_CLUSTER_ID))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        loadResource("schema-registry-rest-mock-responses/get-schema-by-id.json")
                    )
            ));

    var protoSchemaTopicConsumeCCloudResponse = loadResource(
        "message-viewer/ccloud-records-consume-proto.json");

    wireMock.register(
        WireMock
            .post(CCLOUD_SIMPLE_CONSUME_API_PATH.formatted(
                KAFKA_CLUSTER_ID, PROTOBUF_SCHEMA_TOPIC_NAME))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-connection-id", CONNECTION_ID)
                    .withHeader("x-cluster-id", KAFKA_CLUSTER_ID)
                    .withBody(protoSchemaTopicConsumeCCloudResponse)
            )
            .atPriority(100));

    wireMock.register(
        WireMock
            .get(urlMatching(SCHEMAS_BY_ID_URL_REGEX.formatted(ORDERS_PROTOBUF_SCHEMA_ID)))
            .withHeader(
                "Authorization",
                new EqualToPattern("Bearer %s".formatted(getDataPlaneToken()))
            )
            .withHeader("target-sr-cluster", new EqualToPattern(SCHEMA_REGISTRY_CLUSTER_ID))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        loadResource("message-viewer/schema-protobuf.proto")
                    )
            ));
  }
}