package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class FlinkPrivateEndpointUtilTest {

  @Inject
  FlinkPrivateEndpointUtil flinkPrivateEndpointUtil;

  @Test
  void testValidEndpointMatching() {
    // Standard format
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", "aws"));

    // With domain prefix - should match
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.dom123.us-west-2.aws.private.confluent.cloud", "us-west-2", "aws"));

    // HTTP instead of HTTPS - should match
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "http://flink.us-east-1.gcp.private.confluent.cloud", "us-east-1", "gcp"));
  }

  @Test
  void testValidEndpointNotMatching() {
    // Wrong region
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-east-1", "aws"));

    // Wrong provider
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", "gcp"));

    // Both wrong
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "eu-west-1", "azure"));
  }

  @Test
  void testInvalidEndpointFormats() {
    // Wrong subdomain
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://kafka.us-west-2.aws.private.confluent.cloud", "us-west-2", "aws"));

    // Wrong domain
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.confluent.cloud", "us-west-2", "aws"));

    // Missing parts
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.aws.private.confluent.cloud", "us-west-2", "aws"));

    // Invalid URL
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "not-a-url", "us-west-2", "aws"));

    // Wrong TLD
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.com", "us-west-2", "aws"));
  }

  @Test
  void testCaseInsensitivity() {
    // Endpoint region/provider in different cases
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.US-WEST-2.AWS.private.confluent.cloud", "us-west-2", "aws"));

    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "US-WEST-2", "AWS"));

    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.Us-West-2.Aws.private.confluent.cloud", "Us-West-2", "Aws"));
  }

  @Test
  void testNullInputs() {
    // Null endpoint
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        null, "us-west-2", "aws"));

    // Null region
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", null, "aws"));

    // Null provider
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", null));
  }

  @Test
  void testEmptyInputs() {
    // Empty endpoint
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "", "us-west-2", "aws"));

    // Empty region
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "", "aws"));

    // Empty provider
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud", "us-west-2", ""));
  }

  @Test
  void testVariousRegionsAndProviders() {
    // AWS regions
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-east-1.aws.private.confluent.cloud", "us-east-1", "aws"));
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.eu-central-1.aws.private.confluent.cloud", "eu-central-1", "aws"));

    // GCP regions
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-central1.gcp.private.confluent.cloud", "us-central1", "gcp"));

    // Azure regions
    assertTrue(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.eastus.azure.private.confluent.cloud", "eastus", "azure"));
  }

  @Test
  void testEdgeCaseUrls() {
    // With trailing slash (should be handled by normalization upstream)
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud/", "us-west-2", "aws"));

    // With path
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud/api/v1", "us-west-2", "aws"));

    // With query parameters
    assertFalse(flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
        "https://flink.us-west-2.aws.private.confluent.cloud?param=value", "us-west-2", "aws"));
  }
}
