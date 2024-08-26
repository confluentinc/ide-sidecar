package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClusterStrategyTest {

  static class ClusterStrategyImpl extends ClusterStrategy {

  }

  static ClusterStrategyImpl baseClusterStrategy = new ClusterStrategyImpl();

  @ParameterizedTest
  @MethodSource
  void testProcessProxyResponse(
      String proxyResponse,
      String clusterUri,
      String expectedResponse
  ) {
    var actualResponse = baseClusterStrategy.processProxyResponse(
        proxyResponse,
        clusterUri,
        "http://localhost:26637"
    );
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
            "https://pkc-1234.us-west-2.aws.confluent.cloud",
            // http://localhost:26637 is the default sidecar URL
            """
                {
                  "partitions": {
                    "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """
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
            "https://pkc-1234.us-west-2.aws.confluent.cloud",
            """
                {
                  "partitions": {
                    "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """
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
            "https://pkc-1234.us-west-2.aws.confluent.cloud:443",
            """
                {
                  "partitions": {
                    "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """
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

            "https://pkc-1234.us-west-2.aws.confluent.cloud:443",
            """
                {
                  "partitions": {
                    "related": "http://localhost:26637/kafka/v3/clusters/lkc-95w6wy/topics/my_topic/partitions"
                  }
                }
                """
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
