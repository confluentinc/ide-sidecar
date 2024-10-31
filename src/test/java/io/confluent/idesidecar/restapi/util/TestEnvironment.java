/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import java.util.Optional;

/**
 * A test environment that provides a {@link SidecarClient} to interact with the Sidecar REST API.
 */
public interface TestEnvironment extends AutoCloseable {

  void start();

  void shutdown();

  /**
   * Obtain a {@link ConnectionSpec} for a local connection to the Kafka cluster and
   * the Schema Registry cluster in this environment.
   *
   * @return the connection spec, or empty if this environment does not support local connections
   */
  Optional<ConnectionSpec> localConnectionSpec();

  /**
   * Obtain a {@link ConnectionSpec} for a direct connection to the Kafka cluster and
   * the Schema Registry cluster in this environment.
   *
   * @return the connection spec, or empty if this environment does not support local connections
   */
  Optional<ConnectionSpec> directConnectionSpec();

  /**
   * Obtain a {@link ConnectionSpec} for a direct connection to the Kafka cluster and
   * optionally the Schema Registry cluster in this environment.
   *
   * @return the connection spec, or empty if this environment does not support local connections
   */
  default Optional<ConnectionSpec> directConnectionSpec(boolean withSchemaRegistry) {
    var spec = directConnectionSpec();
    if (!withSchemaRegistry) {
      // Remove the SR cluster from the spec
      spec = spec.map(s -> s.withSchemaRegistry(null));
    }
    return spec;
  }
}
