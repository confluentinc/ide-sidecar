package io.confluent.idesidecar.restapi.util;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.UUID;

@ApplicationScoped
public class UuidFactory {

  public String getRandomUuid() {
    return UUID.randomUUID().toString();
  }
}
