package io.confluent.idesidecar.restapi.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ObjectMetadataTest {

  @Test
  void selfShouldReturnAValidApiPath() {
    var objectMetadata = new ObjectMetadata("/gateway/v1/fake-resource-path", "fake-resource-id");
    assertEquals(
        "http://localhost:26637/gateway/v1/fake-resource-path/fake-resource-id",
        objectMetadata.self());
  }

  @Test
  void resourceNameShouldBeNull() {
    var objectMetadata = new ObjectMetadata("/gateway/v1/fake-resource-path", "fake-resource-id");
    assertNull(objectMetadata.resourceName());
  }
}
