package io.confluent.idesidecar.restapi.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Factory class for {@link ObjectMapper}s. Makes sure that we use the correct configuration
 * when deserializing/serializing JSON objects.
 */
public class ObjectMapperFactory {

  /**
   * Creates a new {@link ObjectMapper} instance that ignores unknown properties.
   * @return a configured {@link ObjectMapper} instance
   */
  public static ObjectMapper getObjectMapper() {
    var objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return objectMapper;
  }
}
