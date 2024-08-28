package io.confluent.idesidecar.restapi.util;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.UUID;

/**
 * Factory class for {@link UUID}s.
 */
@ApplicationScoped
public class UuidFactory {

  public String getRandomUuid() {
    return UUID.randomUUID().toString();
  }
}
