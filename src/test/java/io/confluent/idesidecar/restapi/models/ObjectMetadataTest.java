package io.confluent.idesidecar.restapi.models;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ObjectMetadataTest {

  @Test
  void selfShouldReturnAValidApiPath() {
    var resourceName = "fake-resource-path";
    var self = "http://localhost:26637/gateway/v1/fake-resource-path";
    var objectMetadata = new ObjectMetadata(self, resourceName);
    assertEquals(self, objectMetadata.self());
  }
}
