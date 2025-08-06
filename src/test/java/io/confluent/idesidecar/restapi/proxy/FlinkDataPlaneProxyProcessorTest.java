package io.confluent.idesidecar.restapi.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
class FlinkDataPlaneProxyProcessorTest {

  @Inject
  FlinkDataPlaneProxyProcessor processor;

  @InjectMock
  FlinkPrivateEndpointUtil flinkPrivateEndpointUtil;

  @Test
  void testSelectMatchingEndpointFirstMatch() {
    List<String> endpoints = List.of(
        "https://flink.us-west-2.aws.private.confluent.cloud",
        "https://flink.us-east-1.aws.private.confluent.cloud"
    );

    // Mock: first endpoint matches, second doesn't
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", "aws"))
        .thenReturn(true);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-east-1.aws.private.confluent.cloud", "us-west-2", "aws"))
        .thenReturn(false);

    Optional<String> result = processor.selectMatchingEndpoint(endpoints, "us-west-2", "aws");

    assertTrue(result.isPresent());
    assertEquals("https://flink.us-west-2.aws.private.confluent.cloud", result.get());
  }

  @Test
  void testSelectMatchingEndpointLaterMatch() {
    List<String> endpoints = List.of(
        "https://invalid-endpoint.com",
        "flink.eu-west-1.gcp.private.confluent.cloud",
        "https://flink.us-west-2.aws.private.confluent.cloud"
    );

    // Mock: last endpoint matches
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://invalid-endpoint.com", "us-west-2", "aws"))
        .thenReturn(false);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "flink.eu-west-1.gcp.private.confluent.cloud", "us-west-2", "aws"))
        .thenReturn(false);
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", "aws"))
        .thenReturn(true);

    Optional<String> result = processor.selectMatchingEndpoint(endpoints, "us-west-2", "aws");

    assertTrue(result.isPresent());
    assertEquals("https://flink.us-west-2.aws.private.confluent.cloud", result.get());
  }

  @Test
  void testSelectMatchingEndpointNoMatch() {
    List<String> endpoints = List.of(
        "https://flink.us-east-1.aws.private.confluent.cloud",
        "https://flink.eu-west-1.aws.private.confluent.cloud"
    );

    // Mock: no endpoints match
    when(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        anyString(), anyString(), anyString()))
        .thenReturn(false);

    Optional<String> result = processor.selectMatchingEndpoint(endpoints, "us-west-2", "aws");

    assertTrue(result.isEmpty());
  }

  @Test
  void testSelectMatchingEndpointEmptyList() {
    Optional<String> result = processor.selectMatchingEndpoint(List.of(), "us-west-2", "aws");
    assertTrue(result.isEmpty());
  }
}
