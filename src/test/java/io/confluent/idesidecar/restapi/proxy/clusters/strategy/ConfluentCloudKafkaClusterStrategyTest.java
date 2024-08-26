package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
class ConfluentCloudKafkaClusterStrategyTest {

  static ClusterStrategy strategy = new ConfluentCloudKafkaClusterStrategy();

  @ParameterizedTest
  @MethodSource
  void testConstructProxyUri(String requestUri, String clusterUri, String expectedUri) {
    var actualUri = strategy.constructProxyUri(
        requestUri,
        clusterUri
    );
    assertEquals(expectedUri, actualUri);
  }

  private static Stream<Arguments> testConstructProxyUri() {
    return Stream.of(
        Arguments.of("/kafka/v3/clusters/my-cluster/topics",
            "https://pkc-1234.us-west-2.aws.confluent.cloud",
            // Expected URI
            "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/my-cluster/topics"),
        Arguments.of(
            "/kafka/v3/clusters/my-cluster/topics",
            "https://pkc-1234.us-west-2.aws.confluent.cloud/",
            // Expected URI
            "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/my-cluster/topics"
        ),
        Arguments.of(
            "kafka/v3/clusters/my-cluster/topics",
            "https://pkc-1234.us-west-2.aws.confluent.cloud/",
            // Expected URI
            "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/my-cluster/topics"
        ),
        Arguments.of(
            "kafka/v3/clusters/my-cluster/topics",
            "https://pkc-1234.us-west-2.aws.confluent.cloud",
            // Expected URI
            "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/my-cluster/topics"
        ));
  }

}
