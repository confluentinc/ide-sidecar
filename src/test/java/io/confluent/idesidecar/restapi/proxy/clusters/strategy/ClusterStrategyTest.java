package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.quarkus.test.junit.QuarkusTest;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
class ClusterStrategyTest {

  static final int TEST_PORT = ConfigProvider.getConfig()
      .getValue("quarkus.http.test-port", Integer.class);

  static class ClusterStrategyImpl extends ClusterStrategy {

  }

  static ClusterStrategyImpl baseClusterStrategy = new ClusterStrategyImpl();

  @ParameterizedTest
  @MethodSource
  void testProcessProxyResponse(
      String proxyResponse,
      String expectedResponse
  ) {
    var actualResponse = baseClusterStrategy.processProxyResponse(proxyResponse);
    assertEquals(expectedResponse, actualResponse);
  }

  private static Stream<Arguments> testProcessProxyResponse() {
    return Stream.of(
        Arguments.of(

            """
                {
                  "partitions": {
                    "related": "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """,
            // http://localhost:26637 is the default sidecar URL
            """
                {
                  "partitions": {
                    "related": "http://localhost:%s/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """.formatted(TEST_PORT)
        ),
        // When the returned URLs contain a port
        Arguments.of(

            """
                {
                  "partitions": {
                    "related": "https://pkc-1234.us-west-2.aws.confluent.cloud:443/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """,
            """
                {
                  "partitions": {
                    "related": "http://localhost:%s/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """.formatted(TEST_PORT)
        ),
        // When the stored cluster URI has a port, but the returned URLs do not
        Arguments.of(

            """
                {
                  "partitions": {
                    "related": "https://pkc-1234.us-west-2.aws.confluent.cloud/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """,
            """
                {
                  "partitions": {
                    "related": "http://localhost:%s/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """.formatted(TEST_PORT)
        ),
        // When both the stored cluster URI and the returned URLs have ports
        Arguments.of(

            """
                {
                  "partitions": {
                    "related": "https://pkc-1234.us-west-2.aws.confluent.cloud:443/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """,
            """
                {
                  "partitions": {
                    "related": "http://localhost:%s/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """.formatted(TEST_PORT)
        ));
  }

  @Test
  void testConstructKafkaProxyUriWithBadUri() {
    assertThrows(IllegalArgumentException.class,
        () -> baseClusterStrategy.constructProxyUri(
            "/kafka/v3/clusters/my-cluster/topics",
            "this is a bad uri"
        ));
  }
}
