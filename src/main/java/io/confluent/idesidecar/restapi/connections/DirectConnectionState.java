package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of the connection state for ({@link ConnectionType#DIRECT} connections where the
 * Kafka and Schema Registry clusters are provided.
 */
public class DirectConnectionState extends ConnectionState {

  public DirectConnectionState() {
    super(null, null);
  }

  public DirectConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
  }

  public MultiMap getAuthenticationHeaders() {
    // Direct connections do not require authentication headers
    return HttpHeaders.headers();
  }

  @Override
  public Optional<Credentials> getKafkaCredentials(String clusterId) {
    Credentials credentials = spec.kafkaClusterConfig() != null
                              ? spec.kafkaClusterConfig().credentials()
                              : null;
    return Optional.ofNullable(credentials);
  }

  @Override
  public Optional<Credentials> getSchemaRegistryCredentials(String clusterId) {
    Credentials credentials = spec.schemaRegistryConfig() != null
                              ? spec.schemaRegistryConfig().credentials()
                              : null;
    return Optional.ofNullable(credentials);
  }

  // TODO: DIRECT connections need validation checks
}
