package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
class ConfluentLocalKafkaClusterStrategyTest {
  private static final int TEST_PORT = ConfigProvider.getConfig()
      .getValue("quarkus.http.test-port", Integer.class);

  @Inject
  ConfluentLocalKafkaClusterStrategy strategy;

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
            "http://localhost:%s/internal/kafka/v3/clusters/my-cluster/topics".formatted(TEST_PORT)
        ),
        Arguments.of(

            "kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082",
            "http://localhost:%s/internal/kafka/v3/clusters/my-cluster/topics".formatted(TEST_PORT)
        ),
        Arguments.of(

            "kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082/",
            "http://localhost:%s/internal/kafka/v3/clusters/my-cluster/topics".formatted(TEST_PORT)
        ),
        Arguments.of(

            "/kafka/v3/clusters/my-cluster/topics",
            "http://localhost:8082/",
            "http://localhost:%s/internal/kafka/v3/clusters/my-cluster/topics".formatted(TEST_PORT)
        )
    );
  }

  @Test
  void testProcessProxyResponse() {
    String proxyResponse = """
        {
          "partitions": {
            "related": "http://localhost:26637/internal/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
          }
        }
        """;
    String expectedResponse = """
        {
          "partitions": {
            "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
          }
        }
        """;
    var actualResponse = strategy.processProxyResponse(proxyResponse);
    assertEquals(expectedResponse, actualResponse);
  }
}