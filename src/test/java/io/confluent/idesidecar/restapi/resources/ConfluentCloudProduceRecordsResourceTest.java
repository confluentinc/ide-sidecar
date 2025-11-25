package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectKafkaClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectSchemaRegistryForKafkaClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectSchemaRegistryInCache;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CLUSTER_ID_HEADER;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.auth.Token;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@ConnectWireMock
public class ConfluentCloudProduceRecordsResourceTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @InjectMock
  ClusterCache clusterCache;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  Token dataPlaneToken;

  private static final String KAFKA_CLUSTER_ID = "lkc-abcd123";
  private static final String CONNECTION_ID = "fake-connection-id";
  private static final String SCHEMA_REGISTRY_CLUSTER_ID = "lsrc-defg456";
  private static final Map<String, String> KAFKA_CLUSTER_REQUEST_HEADERS = Map.of(
      CLUSTER_ID_HEADER, KAFKA_CLUSTER_ID,
      CONNECTION_ID_HEADER, CONNECTION_ID
  );

  @BeforeEach
  void setUp() {
    // Create authenticated CCloud connection
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionSpec.ConnectionType.CCLOUD);

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

    dataPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
            .getOauthContext()
            .getDataPlaneToken();

    // We need to mock the expected Schema Registry interactions
    wireMock.register(
        WireMock
            .get("/subjects/test-topic-value/versions/1?deleted=false")
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token())))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .withGzipDisabled(true)
                    .withBody(
                        loadResource(
                            "ccloud-produce-records-mocks/get-schema-success-response.json")
                    )
            )
    );
    wireMock.register(
        WireMock
            .post("/subjects/test-topic-value?normalize=false&deleted=false")
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token())))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .withGzipDisabled(true)
                    .withBody(
                        loadResource(
                            "ccloud-produce-records-mocks/get-schema-success-response.json")
                    )
            )
    );
  }

  @AfterEach
  void tearDown() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.removeMappings();
  }

  @Test
  void testProduceRecordToConfluentCloud() {
    // We need to mock the Confluent Cloud REST API interactions
    wireMock.register(
        WireMock
            .post(
                "/kafka/v3/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
            .withRequestBody(
                WireMock.equalToJson(
                    loadResource(
                        "ccloud-produce-records-mocks/produce-record-expected-request.json"),
                    // Ignore order and extra elements
                    true, true
                )
            )
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withGzipDisabled(true)
                    .withBody(
                        loadResource(
                            "ccloud-produce-records-mocks/produce-record-success-response.json")
                    )
            )
    );

    // Let's issue a request to produce a record
    var actualResponse = given()
        .when()
        .headers(KAFKA_CLUSTER_REQUEST_HEADERS)
        .header("Content-Type", "application/json")
        .body(
            ProduceRequest
                .builder()
                .key(
                    ProduceRequestData
                        .builder()
                        .data(1234)
                        .build()
                )
                .value(
                    ProduceRequestData
                        .builder()
                        .data(
                            Map.of(
                                "orderId", "order_7898",
                                "customerId", "cust_456",
                                "orderDate", "2024-01-15T10:15:00Z",
                                "totalAmount", 129.99,
                                "items", new Object[]{
                                    Map.of(
                                        "productId", "prod_001",
                                        "quantity", 2,
                                        "price", 64.99
                                    )
                                }
                            )
                        )
                        .subject("test-topic-value")
                        .subjectNameStrategy("topic_name")
                        .schemaVersion(1)
                        .build()
                )
                .build()
        )
        .post("/gateway/v1/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
        .then();

    // Expected
    actualResponse.statusCode(200);
    actualResponse.body("error_code", is(nullValue()));
    actualResponse.body("message", is(nullValue()));
    actualResponse.body("cluster_id", is("lkc-abcd123"));
    actualResponse.body("topic_name", is("test-topic"));
    actualResponse.body("partition_id", is(0));
    actualResponse.body("offset", is(1));
    actualResponse.body("timestamp", is("2025-02-28T20:56:03.476+00:00"));
    actualResponse.body("key.size", is(4));
    actualResponse.body("key.type", is(nullValue()));
    actualResponse.body("key.subject", is(nullValue()));
    actualResponse.body("key.schema_id", is(nullValue()));
    actualResponse.body("key.schema_version", is(nullValue()));
    actualResponse.body("value.size", is(74));
    actualResponse.body("value.type", is("AVRO"));
    actualResponse.body("value.subject", is("test-produce-value"));
    actualResponse.body("value.schema_id", is(100003));
    actualResponse.body("value.schema_version", is(3));
  }

  @Test
  void testProduceRecordToConfluentCloudWithNullKey() {
    // We need to mock the Confluent Cloud REST API interactions
    wireMock.register(
        WireMock
            .post(
                "/kafka/v3/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
            .withRequestBody(
                WireMock.equalToJson(
                    loadResource(
                        "ccloud-produce-records-mocks/produce-record-expected-request-null-key.json"),
                    // Ignore order and extra elements
                    true, true
                )
            )
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withGzipDisabled(true)
                    .withBody(
                        loadResource(
                            "ccloud-produce-records-mocks/produce-record-success-response.json")
                    )
            )
    );

    // Let's issue a request to produce a record with a null key
    var actualResponse = given()
        .when()
        .headers(KAFKA_CLUSTER_REQUEST_HEADERS)
        .header("Content-Type", "application/json")
        .body(
            ProduceRequest
                .builder()
                .key(
                    ProduceRequestData
                        .builder()
                        .data(null)
                        .build()
                )
                .value(
                    ProduceRequestData
                        .builder()
                        .data(
                            Map.of(
                                "orderId", "order_7898",
                                "customerId", "cust_456",
                                "orderDate", "2024-01-15T10:15:00Z",
                                "totalAmount", 129.99,
                                "items", new Object[]{
                                    Map.of(
                                        "productId", "prod_001",
                                        "quantity", 2,
                                        "price", 64.99
                                    )
                                }
                            )
                        )
                        .subject("test-topic-value")
                        .subjectNameStrategy("topic_name")
                        .schemaVersion(1)
                        .build()
                )
                .build()
        )
        .post("/gateway/v1/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
        .then();

    // Expected
    actualResponse.statusCode(200);
  }

  @Test
  void testProduceRecordToConfluentCloudWithNullValue() {
    // We need to mock the Confluent Cloud REST API interactions
    wireMock.register(
        WireMock
            .post(
                "/kafka/v3/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
            .withRequestBody(
                WireMock.equalToJson(
                    loadResource(
                        "ccloud-produce-records-mocks/produce-record-expected-request-null-value.json"),
                    // Ignore order and extra elements
                    true, true
                )
            )
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withGzipDisabled(true)
                    .withBody(
                        loadResource(
                            "ccloud-produce-records-mocks/produce-record-success-response.json")
                    )
            )
    );

    // Let's issue a request to produce a record with a null value
    var actualResponse = given()
        .when()
        .headers(KAFKA_CLUSTER_REQUEST_HEADERS)
        .header("Content-Type", "application/json")
        .body(
            ProduceRequest
                .builder()
                .key(
                    ProduceRequestData
                        .builder()
                        .data(1234)
                        .build()
                )
                .value(
                    ProduceRequestData
                        .builder()
                        .data(null)
                        .subject("test-topic-value")
                        .subjectNameStrategy("topic_name")
                        .schemaVersion(1)
                        .build()
                )
                .build()
        )
        .post("/gateway/v1/clusters/%s/topics/%s/records".formatted(KAFKA_CLUSTER_ID, "test-topic"))
        .then();

    // Expected
    actualResponse.statusCode(200);
  }
}
