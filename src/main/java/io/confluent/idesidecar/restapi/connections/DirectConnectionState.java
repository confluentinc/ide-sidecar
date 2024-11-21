package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.auth.AuthErrors;
import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.KafkaClusterStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.SchemaRegistryStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatusBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionStatusKafkaClusterStatusBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionStatusSchemaRegistryStatusBuilder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.quarkus.logging.Log;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Implementation of the connection state for ({@link ConnectionType#DIRECT} connections where the
 * Kafka and Schema Registry clusters are provided.
 */
public class DirectConnectionState extends ConnectionState {

  static final Duration TIMEOUT = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getOptionalValue("ide-sidecar.connections.direct.timeout-seconds", Long.class)
          .orElse(5L)
  );

  static final ConnectionStatus INITIAL_STATUS = new ConnectionStatus(
      null,
      new KafkaClusterStatus(
          ConnectedState.ATTEMPTING,
          null,
          null
      ),
      new SchemaRegistryStatus(
          ConnectedState.ATTEMPTING,
          null,
          null
      )
  );

  public DirectConnectionState() {
    super(null, null);
  }

  public DirectConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
  }

  @Override
  public ConnectionStatus getInitialStatus() {
    return INITIAL_STATUS;
  }

  public MultiMap getAuthenticationHeaders(ClusterType clusterType) {
    var headers = HttpHeaders.headers();
    var credentials = switch (clusterType) {
      case KAFKA ->
          spec.kafkaClusterConfig() != null
          ? spec.kafkaClusterConfig().credentials()
          : null;
      case SCHEMA_REGISTRY ->
          spec.schemaRegistryConfig() != null
          ? spec.schemaRegistryConfig().credentials()
          : null;
      default -> null;
    };
    if (credentials != null) {
      credentials.httpClientHeaders().ifPresent(map -> map.forEach(headers::add));
    }
    return headers;
  }

  @Override
  public Optional<Credentials> getKafkaCredentials() {
    Credentials credentials = spec.kafkaClusterConfig() != null
                              ? spec.kafkaClusterConfig().credentials()
                              : null;
    return Optional.ofNullable(credentials);
  }

  @Override
  public Optional<Credentials> getSchemaRegistryCredentials() {
    Credentials credentials = spec.schemaRegistryConfig() != null
                              ? spec.schemaRegistryConfig().credentials()
                              : null;
    return Optional.ofNullable(credentials);
  }

  @Override
  public Future<ConnectionStatus> getConnectionStatus() {
    return Future.join(
        getKafkaConnectionStatus(),
        getSchemaRegistryConnectionStatus()
    ).map(cf -> {
      var futures = cf.list();
      var kafkaStatus = (KafkaClusterStatus) futures.get(0);
      var srStatus = (SchemaRegistryStatus) futures.get(1);
      return ConnectionStatusBuilder
          .builder()
          .kafkaCluster(kafkaStatus)
          .schemaRegistry(srStatus)
          .build();
    });
  }

  protected Future<KafkaClusterStatus> getKafkaConnectionStatus() {
    return withAdminClient(adminClient -> {
      // There is a Kafka configuration, so validate the connection by creating an AdminClient
      // and describing the cluster.
      try {
        var clusterId = adminClient
            .describeCluster()
            .clusterId()
            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return Future.succeededFuture(
            ConnectionStatusKafkaClusterStatusBuilder
                .builder()
                .state(ConnectedState.SUCCESS)
                .build()
        );
      } catch (Exception e) {
        // The connection failed
        Throwable cause = e;
        if (e instanceof java.util.concurrent.ExecutionException) {
          cause = e.getCause();
        }
        // so successfully return the failed state
        return Future.succeededFuture(
            ConnectionStatusKafkaClusterStatusBuilder
                .builder()
                .state(ConnectedState.FAILED)
                .errors(
                    new AuthErrors().withSignIn(
                        "Failed to connect to Kafka cluster: %s".formatted(cause.getMessage())
                    )
                ).build()
        );
      }
    }).orElseGet(() -> {
      // There is no Kafka cluster configuration, so return the NONE state
      return Future.succeededFuture(
          ConnectionStatusKafkaClusterStatusBuilder
              .builder()
              .state(ConnectedState.NONE)
              .build()
      );
    });
  }

  protected Future<SchemaRegistryStatus> getSchemaRegistryConnectionStatus() {
    return withSchemaRegistryClient(srClient -> {
      // There is a configuration, so validate the connection by creating a SchemaRegistryClient
      // and getting the global mode.
      try {
        srClient.getMode();
        return Future.succeededFuture(
            ConnectionStatusSchemaRegistryStatusBuilder
                .builder()
                .state(ConnectedState.SUCCESS)
                .build()
        );
      } catch (Exception e) {
        // The connection failed, so successfully return the failed state
        return Future.succeededFuture(
            ConnectionStatusSchemaRegistryStatusBuilder
                .builder()
                .state(ConnectedState.FAILED)
                .errors(
                    new AuthErrors().withSignIn(
                        "Failed to connect to Schema Registry: %s".formatted(e.getMessage())
                    )
                ).build()
        );
      }
    }).orElseGet(() -> {
      // There is no Schema Registry configuration, so return the NONE state
      return Future.succeededFuture(
          ConnectionStatusSchemaRegistryStatusBuilder
              .builder()
              .state(ConnectedState.NONE)
              .build()
          );
    });
  }

  /**
   * If there is a Kafka cluster configuration, then execute the given operation with
   * an {@link AdminClient}. The operation should handle exceptions.
   *
   * @param operation the function to execute with the AdminClient
   * @return the result of the operation, or empty if the operation was not called or threw
   *         an exception
   */
  public <T> Optional<T> withAdminClient(Function<AdminClient, T> operation) {
    var kafkaClusterConfig = spec.kafkaClusterConfig();
    if (kafkaClusterConfig == null) {
      return Optional.empty();
    }
    try (var adminClient = createAdminClient(kafkaClusterConfig)) {
      return Optional.ofNullable(
          operation.apply(adminClient)
      );
    } catch (Exception e) {
      Log.errorf("Failed to create and call AdminClient client: %s", e.getMessage(), e);
      return Optional.empty();
    }
  }

  AdminClient createAdminClient(
      ConnectionSpec.KafkaClusterConfig config
  ) {
    // Create the configuration for an AdminClient
    var adminConfig = ClientConfigurator.getKafkaClientConfig(
        this,
        config.bootstrapServers(),
        null,
        false,
        null,
        Map.of()
    );
    return AdminClient.create(adminConfig);
  }

  /**
   * If there is a Schema Registry configuration, then execute the given operation with
   * an {@link SchemaRegistryClient}. The operation should handle exceptions.
   *
   * @param operation the function to execute with the client
   * @return the result of the operation, or empty if the operation was not called or threw
   *         an exception
   */
  public <T> Optional<T> withSchemaRegistryClient(Function<SchemaRegistryClient, T> operation) {
    var srConfig = spec.schemaRegistryConfig();
    if (srConfig == null) {
      return Optional.empty();
    }
    try (var srClient = createSchemaRegistryClient(srConfig)) {
      return Optional.ofNullable(
          operation.apply(srClient)
      );
    } catch (Exception e) {
      Log.errorf("Failed to create and call Schema Registry client: %s", e.getMessage(), e);
      return Optional.empty();
    }
  }

  SchemaRegistryClient createSchemaRegistryClient(
      ConnectionSpec.SchemaRegistryConfig config
  ) {
    var srClientConfig = ClientConfigurator.getSchemaRegistryClientConfig(
        this,
        config.uri(),
        false,
        TIMEOUT
    );
    return new CachedSchemaRegistryClient(
        Collections.singletonList(config.uri()),
        10,
        srClientConfig
    );
  }
}
