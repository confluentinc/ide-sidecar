package io.confluent.idesidecar.restapi.connections;

import static io.confluent.idesidecar.restapi.util.ExceptionUtil.unwrap;

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
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.quarkus.logging.Log;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.ConfigException;
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

  static final KafkaClusterStatus KAFKA_INITIAL_STATUS = new KafkaClusterStatus(
      ConnectedState.ATTEMPTING,
      null,
      null
  );

  static final SchemaRegistryStatus SR_INITIAL_STATUS = new SchemaRegistryStatus(
      ConnectedState.ATTEMPTING,
      null,
      null
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
  protected ConnectionStatus getInitialStatus() {
    return new ConnectionStatus(
        null,
        spec.kafkaClusterConfig() != null ? KAFKA_INITIAL_STATUS : null,
        spec.schemaRegistryConfig() != null ? SR_INITIAL_STATUS : null
    );
  }

  /**
   * Check if the connection to the Kafka cluster is currently connected.
   *
   * @return true if the Kafka component is connected
   */
  public boolean isKafkaConnected() {
    var status = getStatus();
    return status.kafkaCluster() != null && status.kafkaCluster().isConnected();
  }

  /**
   * Check if the connection to the Schema Registry is currently connected.
   *
   * @return true if the Kafka component is connected
   */
  public boolean isSchemaRegistryConnected() {
    var status = getStatus();
    return status.schemaRegistry() != null && status.schemaRegistry().isConnected();
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
  protected Future<ConnectionStatus> doRefreshStatus() {
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
    return withAdminClient(
        adminClient -> {
          // There is a Kafka configuration, so validate the connection by creating an AdminClient
          // and describing the cluster.
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
        },
        error -> {
          // The connection failed in some way
          var cause = unwrap(error);
          var message = "Unable to connect to Kafka cluster at %s: %s".formatted(
              spec.kafkaClusterConfig().bootstrapServers(),
              cause.getMessage()
          );
          if (cause instanceof ConfigException) {
            message = ("Unable to reach the Kafka cluster at %s. "
                       + "Check the bootstrap server addresses."
            ).formatted(
                spec.kafkaClusterConfig().bootstrapServers()
            );
          } else if (cause instanceof TimeoutException) {
            message = ("Unable to connect to the Kafka cluster at %s."
                + " Check the credentials or the network."
            ).formatted(
                spec.kafkaClusterConfig().bootstrapServers()
            );
          }
          return Future.succeededFuture(
              ConnectionStatusKafkaClusterStatusBuilder
                  .builder()
                  .state(ConnectedState.FAILED)
                  .errors(
                      new AuthErrors().withSignIn(message)
                  ).build()
          );
        }
    ).orElseGet(
        () -> {
            // There is no Kafka cluster configuration, so return a null Kafka status
            return Future.succeededFuture(null);
        }
    );
  }

  protected Future<SchemaRegistryStatus> getSchemaRegistryConnectionStatus() {
    return withSchemaRegistryClient(srClient -> {
      // There is a configuration, so validate the connection by creating a SchemaRegistryClient
      // and getting the global mode.
      srClient.getMode();
      return Future.succeededFuture(
          ConnectionStatusSchemaRegistryStatusBuilder
              .builder()
              .state(ConnectedState.SUCCESS)
              .build()
      );
    }, error -> {
      var cause = unwrap(error);
      var message = "Failed to connect to Schema Registry: %s".formatted(cause.getMessage());
      if (cause instanceof UnknownHostException) {
        message = "Unable to resolve the Schema Registry URL %s".formatted(
            spec.schemaRegistryConfig().uri()
        );
      } else if (cause instanceof IOException || cause instanceof RestClientException) {
        message = "Unable to reach the Schema Registry URL %s".formatted(
            spec.schemaRegistryConfig().uri()
        );
      }
      // The connection failed, so successfully return the failed state
      return Future.succeededFuture(
          ConnectionStatusSchemaRegistryStatusBuilder
              .builder()
              .state(ConnectedState.FAILED)
              .errors(
                  new AuthErrors().withSignIn(message)
              ).build()
      );
    }).orElseGet(() -> {
      // There is no Schema Registry configuration, so return no status for SR
      return Future.succeededFuture(null);
    });
  }

  public interface ClientOperation<ClientT, T> {
    T apply(ClientT client) throws Exception;
  }

  /**
   * If there is a Kafka cluster configuration, then execute the given operation with
   * an {@link AdminClient}. The operation can handle exceptions, or it can handle all exceptions
   * through the supplied error handler.
   *
   * @param operation    the function to execute with the AdminClient
   * @param errorHandler the function that will be called if the client cannot be created or if
   *                     the operation threw an exception
   * @return the result of the operation, or empty if the operation was not called because
   *         there is no Kafka cluster configuration
   */
  public <T> Optional<T> withAdminClient(
      ClientOperation<AdminClient, T> operation,
      Function<Throwable, T> errorHandler
  ) {
    var kafkaClusterConfig = spec.kafkaClusterConfig();
    if (kafkaClusterConfig == null) {
      return Optional.empty();
    }
    AdminClient adminClient = null;
    try {
      adminClient = createAdminClient(kafkaClusterConfig);
      return Optional.ofNullable(
          operation.apply(adminClient)
      );
    } catch (Throwable e) {
      Log.debugf(
          "Failed to connect to the Kafka cluster at %s: %s",
          kafkaClusterConfig.bootstrapServers(),
          e.getMessage(),
          e
      );
      return Optional.ofNullable(
          errorHandler.apply(e)
      );
    } finally {
      if (adminClient != null) {
        try {
          adminClient.close(TIMEOUT);
        } catch (Throwable e) {
          Log.errorf(
              "Error closing the client to the Kafka cluster at %s: %s",
              kafkaClusterConfig.bootstrapServers(),
              e.getMessage(),
              e
          );
        }
      }
    }
  }

  protected AdminClient createAdminClient(
      ConnectionSpec.KafkaClusterConfig config
  ) {
    // Create the configuration for an AdminClient
    var adminConfig = ClientConfigurator.getKafkaAdminClientConfig(
        this,
        config.bootstrapServers(),
        TIMEOUT
    );
    return AdminClient.create(adminConfig.asMap());
  }

  /**
   * If there is a Schema Registry configuration, then execute the given operation with
   * an {@link SchemaRegistryClient}. The operation can handle exceptions, or it can handle all exceptions
   * through the supplied error handler.
   *
   * @param operation    the function to execute with the AdminClient
   * @param errorHandler the function that will be called if the client cannot be created or if
   *                     the operation threw an exception
   * @return the result of the operation, or empty if the operation was not called because
   *         there is no Schema Registry configuration
   */
  public <T> Optional<T> withSchemaRegistryClient(
      ClientOperation<SchemaRegistryClient, T> operation,
      Function<Throwable, T> errorHandler
  ) {
    var srConfig = spec.schemaRegistryConfig();
    if (srConfig == null) {
      return Optional.empty();
    }
    SchemaRegistryClient srClient = null;
    try {
      srClient = createSchemaRegistryClient(srConfig);
      return Optional.ofNullable(
          operation.apply(srClient)
      );
    } catch (Throwable e) {
      Log.debugf(
          "Failed to connect to the Schema Registry at %s: %s",
          srConfig.uri(),
          e.getMessage(),
          e
      );
      return Optional.ofNullable(
          errorHandler.apply(e)
      );
    } finally {
      if (srClient != null) {
        try {
          srClient.close();
        } catch (Throwable e) {
          Log.errorf(
              "Error closing the client to the Schema Registry at %s: %s",
              srConfig.uri(),
              e.getMessage(),
              e
          );
        }
      }
    }
  }

  protected SchemaRegistryClient createSchemaRegistryClient(
      ConnectionSpec.SchemaRegistryConfig config
  ) {
    var srClientConfig = ClientConfigurator.getSchemaRegistryClientConfig(
        this,
        config.uri(),
        false,
        TIMEOUT
    );
    var restService = new RestService(config.uri());
    restService.configure(srClientConfig);
    var sslFactory = new SslFactory(srClientConfig);
    if (sslFactory.sslContext() != null) {
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
    }
    return new CachedSchemaRegistryClient(restService, 10);
  }
}
