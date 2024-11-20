package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.auth.AuthErrors;
import io.confluent.idesidecar.restapi.cache.ClientConfigurator;
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

  public DirectConnectionState() {
    super(null, null);
  }

  public DirectConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
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
    var kafkaConfig = spec.kafkaClusterConfig();
    if (kafkaConfig == null) {
      return Future.succeededFuture(
          ConnectionStatusKafkaClusterStatusBuilder
              .builder()
              .state(ConnectedState.NONE)
              .build()
      );
    }
    // There is a Kafka configuration, so validate the connection by creating an AdminClient
    // and describing the cluster.
    try (var adminClient = createAdminClient(kafkaConfig)) {
      var clusterDesc = adminClient.describeCluster();
      var actualClusterId = clusterDesc.clusterId().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      // Set the cluster ID
      getSpec().kafkaClusterConfig().withId(actualClusterId);
      return Future.succeededFuture(
          ConnectionStatusKafkaClusterStatusBuilder
              .builder()
              .state(ConnectedState.SUCCESS)
              .build()
      );
    } catch (Exception e) {
      // The connection failed, so successfully return the failed state
      return Future.succeededFuture(
          ConnectionStatusKafkaClusterStatusBuilder
              .builder()
              .state(ConnectedState.FAILED)
              .errors(
                  new AuthErrors().withSignIn(
                      "Failed to connect to Kafka cluster: %s".formatted(e.getMessage())
                  )
              ).build()
      );
    }
  }

  protected Future<SchemaRegistryStatus> getSchemaRegistryConnectionStatus() {

    var schemaRegistryConfig = spec.schemaRegistryConfig();
    if (schemaRegistryConfig == null) {
      return Future.succeededFuture(
          ConnectionStatusSchemaRegistryStatusBuilder
              .builder()
              .state(ConnectedState.NONE)
              .build()
      );
    }
    // There is a Schema Registry configuration, so validate the connection by creating a
    // SchemaRegistryClient and getting the mode.
    try (var srClient = createSchemaRegistryClient(schemaRegistryConfig)) {
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
