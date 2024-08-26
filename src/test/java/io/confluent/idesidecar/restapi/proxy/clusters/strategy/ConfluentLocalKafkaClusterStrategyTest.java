package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
class ConfluentLocalKafkaClusterStrategyTest {
  private static final String CONFLUENT_LOCAL_KAFKAREST_HOSTNAME = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.confluent-local.default.kafkarest-hostname", String.class);

  ClusterStrategy strategy;

  @BeforeEach
  void setUp() {
    strategy = new ConfluentLocalKafkaClusterStrategy(CONFLUENT_LOCAL_KAFKAREST_HOSTNAME);
  }

  @ParameterizedTest
  @MethodSource
  void testConstructProxyUri(String requestUri, String kafkaClusterUri,
      String expectedUri) {
    var actualUri = strategy.constructProxyUri(
        requestUri,
        kafkaClusterUri
    );
    assertEquals(expectedUri, actualUri);
  }

  private static Stream<Arguments> testConstructProxyUri() {
    return Stream.of(
        Arguments.of(

            "/kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082",
            "http://localhost:8082/v3/clusters/my-cluster/topics"
        ),
        Arguments.of(

            "kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082",
            "http://localhost:8082/v3/clusters/my-cluster/topics"
        ),
        Arguments.of(

            "kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082/",
            "http://localhost:8082/v3/clusters/my-cluster/topics"
        ),
        Arguments.of(

            "/kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082/",
            "http://localhost:8082/v3/clusters/my-cluster/topics"
        )
    );
  }

  @Test
  void testProcessProxyResponse() {
    String proxyResponse = """
        {
          "partitions": {
            "related": "http://rest-proxy:8082/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
          }
        }
        """;
    String clusterUri = "http://rest-proxy:8082";
    String expectedResponse = """
        {
          "partitions": {
            "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
          }
        }
        """;
    var actualResponse = strategy.processProxyResponse(
        proxyResponse,
        clusterUri,
        "http://localhost:26637"
    );
    assertEquals(expectedResponse, actualResponse);
  }
}