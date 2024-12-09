package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.idesidecar.restapi.util.CCloud.CloudProvider;
import io.confluent.idesidecar.restapi.util.CCloud.CloudProviderRegion;
import io.confluent.idesidecar.restapi.util.CCloud.KafkaClusterIdentifier;
import io.confluent.idesidecar.restapi.util.CCloud.LsrcId;
import io.confluent.idesidecar.restapi.util.CCloud.NetworkId;
import io.confluent.idesidecar.restapi.util.CCloud.Routing;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class CCloudTest {

  @Nested
  public class RoutingTests {

    @Test
    void shouldParseRouting() {
      assertEquals(Routing.GLB, Routing.parse("glb"));
      assertEquals(Routing.PRIVATE, Routing.parse("private"));
      assertEquals(Routing.INTERNAL, Routing.parse("internal"));
    }

    @Test
    void shouldParseRoutingWithDifferentCase() {
      assertEquals(Routing.GLB, Routing.parse("GLB"));
      assertEquals(Routing.PRIVATE, Routing.parse("Private"));
      assertEquals(Routing.INTERNAL, Routing.parse("Internal"));
    }
  }

  @Nested
  public class KafkaEndpointTests {

    @TestFactory
    Stream<DynamicTest> shouldTestUris() {
      record TestCase(
          String displayName,
          String clusterId,
          String networkId,
          String region,
          String cloudProvider,
          String routing,
          String domain,
          String expectedUri,
          String expectedBootstrapServers,
          String lsrcId,
          String expectedSchemaRegistryUri
      ) {

      }

      var inputs = List.of(
          new TestCase(
              "No network, no routing",
              "pkc-1234",
              null,
              "us-west-1",
              "aws",
              null,
              "confluent.cloud",
              "https://pkc-1234.us-west-1.aws.confluent.cloud:443",
              "pkc-1234.us-west-1.aws.confluent.cloud:9092",
              "lsrc-123",
              "https://lsrc-123.us-west-1.aws.confluent.cloud:443"
          ),
          new TestCase(
              "Network and routing",
              "pkc-1234",
              "n1234",
              "us-west-1",
              "aws",
              "glb",
              "confluent.cloud",
              "https://pkc-1234-n1234.us-west-1.aws.glb.confluent.cloud:443",
              "pkc-1234-n1234.us-west-1.aws.glb.confluent.cloud:9092",
              "lsrc-123",
              "https://lsrc-123.us-west-1.aws.confluent.cloud:443"
          ),
          new TestCase(
              "GCP no network, no routing",
              "pkc-1234",
              null,
              "us-west1",
              "gcp",
              null,
              "confluent.cloud",
              "https://pkc-1234.us-west1.gcp.confluent.cloud:443",
              "pkc-1234.us-west1.gcp.confluent.cloud:9092",
              "lsrc-123",
              "https://lsrc-123.us-west1.gcp.confluent.cloud:443"
          )

      );

      return inputs
          .stream()
          .map(input -> DynamicTest.dynamicTest(
              "Testing: " + input.displayName,
              () -> {
                // Construct a KafkaEndpoint object from the input
                var network = Optional.ofNullable(
                    input.networkId != null ? new NetworkId(input.networkId) : null);
                var routing = Optional.ofNullable(
                    input.routing != null ? Routing.parse(input.routing) : null);
                var endpoint = new CCloud.KafkaEndpoint(
                    KafkaClusterIdentifier.parse(input.clusterId).orElseThrow(),
                    network,
                    new CloudProviderRegion(input.region),
                    new CloudProvider(input.cloudProvider),
                    routing,
                    input.domain
                );

                assertEquals(input.expectedUri, endpoint.getUri().toString());
                assertEquals(input.expectedBootstrapServers, endpoint.getBootstrapServers());

                if (input.lsrcId == null) {
                  assertThrows(IllegalArgumentException.class, () -> {
                    var srEndpoint = endpoint.getSchemaRegistryEndpoint(new LsrcId(input.lsrcId));
                  });
                } else {
                  var srEndpoint = endpoint.getSchemaRegistryEndpoint(new LsrcId(input.lsrcId));
                  assertEquals(input.expectedSchemaRegistryUri, srEndpoint.getUri().toString());

                  var srEndpoint2 = CCloud.SchemaRegistryEndpoint.fromUri(
                      srEndpoint.getUri().toString());
                  assertEquals(input.expectedSchemaRegistryUri,
                      srEndpoint2.orElseThrow().getUri().toString());
                }
              })
          );
    }
  }
}