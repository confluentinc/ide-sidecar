package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;

@RegisterForReflection
public abstract class ConfluentLocalRestClient extends ConfluentRestClient {

  @Override
  protected MultiMap headersFor(String connectionId) {
    // Confluent local connections do not require authentication
    return HttpHeaders.headers();
  }
}
