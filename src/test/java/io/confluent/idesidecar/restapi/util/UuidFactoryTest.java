package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class UuidFactoryTest {

  @Test
  void getRandomUuidShouldReturnValidUuid() {
    var uuidFactory = new UuidFactory();

    assertDoesNotThrow(() -> UUID.fromString(uuidFactory.getRandomUuid()));
  }
}
