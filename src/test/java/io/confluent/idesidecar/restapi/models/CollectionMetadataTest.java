package io.confluent.idesidecar.restapi.models;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CollectionMetadataTest {

  @Test
  void selfShouldReturnAValidApiPath() {
    var collectionMetadata = CollectionMetadata.from(
        0,
        "/gateway/v1/fake-resource-path"
    );

    assertEquals(
        "http://localhost:26637/gateway/v1/fake-resource-path",
        collectionMetadata.self()
    );
  }
}
