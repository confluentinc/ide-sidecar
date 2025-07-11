package io.confluent.idesidecar.restapi.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ObjectMapperFactoryTest {

  @Test
  void getObjectMapperShouldReturnConfiguredObjectMapper() {
    var objectMapper = ObjectMapperFactory.getObjectMapper();

    Assertions.assertFalse(
        objectMapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    );
  }
}
