package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.cache.AdminClients;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.vertx.core.http.HttpServerRequest;
import jakarta.inject.Inject;
import java.util.function.Supplier;


/**
 * Abstract base class for all managers. Contains common dependencies and methods. Inheritors must
 * be annotated with {@link jakarta.enterprise.context.RequestScoped} to allow injection of
 * {@link HttpServerRequest}.
 */
public abstract class Manager {
  @Inject
  AdminClients adminClients;

  @Inject
  ClusterCache clusterCache;

  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);
}
