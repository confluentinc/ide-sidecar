package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.events.ClusterKind;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.events.ServiceKind;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ResourceFetchingException;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.LocalConfig;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * A {@link LocalFetcher} that uses the {@link ConnectionStateManager} to find local
 * {@link Connection} and associated resources.
 *
 * <p>This fetcher makes use of
 * <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * so that other components can observe changes in the loaded {@link Cluster} instances.
 * Each event has the following {@link Lifecycle} qualifier:
 * <ul>
 *   <li>{@link Lifecycle.Updated}</li>
 * </ul>
 * and the following {@link ServiceKind} qualifiers:
 * <ul>
 *   <li>{@link ServiceKind.Local}</li>
 * </ul>
 * and one of the following {@link ClusterKind} qualifiers (depending on the kind of cluster):
 * <ul>
 *   <li>{@link ClusterKind.Kafka}</li>
 *   <li>{@link ClusterKind.SchemaRegistry}</li>
 * </ul>
 *
 * @see ClusterEvent
 */
@ApplicationScoped
@RegisterForReflection
public class RealLocalFetcher extends ConfluentLocalRestClient implements LocalFetcher {

  static final String CONFLUENT_LOCAL_KAFKAREST_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.confluent-local.default.kafkarest-uri", String.class);
  static final String CONFLUENT_LOCAL_KAFKAREST_HOSTNAME = ConfigProvider
      .getConfig()
      .getValue(
          "ide-sidecar.connections.confluent-local.default.kafkarest-hostname", String.class);
  static final String CONFLUENT_LOCAL_CLUSTER_NAME = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.confluent-local.default.cluster-name", String.class);
  static final String CONFLUENT_LOCAL_CLUSTERS_URI = ConfigProvider
      .getConfig()
      .getValue(
          "ide-sidecar.connections.confluent-local.resources.clusters-list-uri", String.class);
  static final String CONFLUENT_LOCAL_BROKERS_URI = ConfigProvider
      .getConfig()
      .getValue(
          "ide-sidecar.connections.confluent-local.resources.brokers-list-uri", String.class);

  static final String CONFLUENT_LOCAL_BROKER_ADVERTISED_LISTENER_URI = ConfigProvider
      .getConfig()
      .getValue(
          "ide-sidecar.connections.confluent-local.resources.broker-adv-listeners-config-uri",
          String.class
      );

  static final String CONFLUENT_LOCAL_DEFAULT_SCHEMA_REGISTRY_URI = ConfigProvider
      .getConfig()
      .getValue(
          "ide-sidecar.connections.confluent-local.default.schema-registry-uri",
          String.class
      );

  /**
   * The pattern to find {@code localhost:&lt;port>} where {@code &lt;port>} is 2 to 7 digits only.
   */
  private static final Pattern LOCAL_ADDRESS = Pattern.compile("localhost:\\d{2,7}(?!\\d)");

  @Inject
  Event<ClusterEvent> clusterEvents;

  public List<LocalConnection> getConnections() {
    return connections
        .getConnectionStates()
        .stream()
        .filter(connection -> ConnectionType.LOCAL.equals(connection.getSpec().type()))
        .map(connection -> new LocalConnection(connection.getSpec().id()))
        .toList();
  }

  @Override
  public Uni<ConfluentLocalKafkaCluster> getKafkaCluster(String connectionId) {
    var uri = CONFLUENT_LOCAL_CLUSTERS_URI;
    return listItems(connectionId, uri, null, this::parseKafkaClusterList)
        // Return null if Confluent Local is not available
        .onFailure(ConnectException.class).recoverWithItem(throwable -> null)
        .map(cluster -> cluster.withConnectionId(connectionId))
        .collect()
        .first()
        .onItem()
        .ifNotNull()
        // On the first one, transform to add broker details
        .transformToUni(this::withBrokerDetails)
        .map(cluster -> onLoad(connectionId, cluster));
  }

  <ClusterT extends Cluster> ClusterT onLoad(String connectionId, ClusterT cluster) {
    // Fire an event for this cluster
    ClusterEvent.onLoad(
        clusterEvents,
        cluster,
        ConnectionType.LOCAL,
        connectionId
    );
    return cluster;
  }

  Uni<ConfluentLocalKafkaCluster> withBrokerDetails(ConfluentLocalKafkaCluster cluster) {
    var connectionId = cluster.connectionId();
    var uri = CONFLUENT_LOCAL_BROKERS_URI.formatted(cluster.id());
    try {
      return listItems(connectionId, uri, null, this::parseKafkaBrokerList)
          // Return null if Confluent Local is not available
          .onFailure(ConnectException.class)
          .recoverWithItem(throwable -> null)
          .onItem()
          .transformToUniAndConcatenate(broker -> {
            var url = CONFLUENT_LOCAL_BROKER_ADVERTISED_LISTENER_URI.formatted(
                cluster.id(), broker.brokerId()
            );
            return getItem(connectionId, url, this::parseConfigResponse);
          })
          .collect()
          .asList()
          .map(configs -> asLocalCluster(cluster.id(), configs));
    } catch (ConnectionNotFoundException | ResourceFetchingException e) {
      return Uni.createFrom().failure(e);
    }
  }

  public Uni<LocalSchemaRegistry> getSchemaRegistry(
      String connectionId
  ) {
    var localConfig = connections.getConnectionSpec(connectionId).localConfig();
    String uri = Optional.ofNullable(localConfig)
        .map(LocalConfig::schemaRegistryUri)
        .filter(url -> !url.isBlank())
        .orElse(CONFLUENT_LOCAL_DEFAULT_SCHEMA_REGISTRY_URI);

    if (uri == null || uri.isBlank()) {
      Log.warnf("Schema Registry URL is missing or blank for connection: %s", connectionId);
      return Uni.createFrom().nullItem();
    }

    final String configUri = uri + "/config";
    return getItem(connectionId, configUri, this::parseSchemaRegistryConfig)
        .onFailure(ConnectException.class).recoverWithItem(throwable -> null)
        .onItem()
        .transformToUni(response -> {
          // Verify the compatibility level exists
          if (response == null || response.compatibilityLevel() == null) {
            Log.infof("Request to schema-registry '%s' failed.", configUri);
            return Uni.createFrom().nullItem();
          }
          return Uni.createFrom().item(() -> new LocalSchemaRegistry(uri, connectionId));
        })
        .map(localSchemaRegistry -> onLoad(connectionId, localSchemaRegistry));
  }

  SchemaRegistryConfigResponse parseSchemaRegistryConfig(String url, String json) {
    return parseRawItem(url, json, SchemaRegistryConfigResponse.class);
  }

  PageOfResults<ConfluentLocalKafkaCluster> parseKafkaClusterList(
      String json,
      PaginationState state
  ) {
    return parseList(json, state, KafkaClusterListResponse.class);
  }

  PageOfResults<KafkaBrokerResponse> parseKafkaBrokerList(
      String json,
      PaginationState state
  ) {
    return parseList(json, state, KafkaBrokerListResponse.class);
  }

  KafkaBrokerConfigResponse parseConfigResponse(String url, String json) {
    return parseRawItem(url, json, KafkaBrokerConfigResponse.class);
  }

  static ConfluentLocalKafkaCluster asLocalCluster(
      String clusterId,
      List<KafkaBrokerConfigResponse> configs
  ) {
    var localAddresses = configs
        .stream()
        .map(KafkaBrokerConfigResponse::value)
        .map(RealLocalFetcher::extractLocalAddresses)
        .flatMap(List::stream)
        .sorted()
        .toList();
    return new ConfluentLocalKafkaCluster(
        clusterId,
        CONFLUENT_LOCAL_CLUSTER_NAME,
        CONFLUENT_LOCAL_KAFKAREST_URI,
        CONFLUENT_LOCAL_KAFKAREST_HOSTNAME,
        String.join(",", localAddresses)
    );
  }

  static List<String> extractLocalAddresses(String advertisedListeners) {
    List<String> results = new ArrayList<>();
    if (advertisedListeners != null && !advertisedListeners.isBlank()) {
      var matcher = LOCAL_ADDRESS.matcher(advertisedListeners);
      while (matcher.find()) {
        results.add(matcher.group(0));
      }
    }
    return results;
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterListResponse(
      String kind,
      ListMetadata metadata,
      @JsonProperty(required = true) List<KafkaClusterResponse> data
  ) implements ListResponse<KafkaClusterResponse, ConfluentLocalKafkaCluster> {
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterResponse(
      String kind,
      JsonNode metadata,
      @JsonProperty(value = "cluster_id", required = true) String clusterId,
      KafkaClusterRelated controller,
      KafkaClusterRelated acls,
      KafkaClusterRelated brokers,
      @JsonProperty("broker_configs") KafkaClusterRelated brokerConfigs,
      @JsonProperty("consumer_groups") KafkaClusterRelated consumerGroups,
      KafkaClusterRelated topics,
      @JsonProperty("partition_reassignments") KafkaClusterRelated partitionReassignments
  ) implements ListItem<ConfluentLocalKafkaCluster> {

    @Override
    public ConfluentLocalKafkaCluster toRepresentation() {
      return new ConfluentLocalKafkaCluster(
          clusterId,
          CONFLUENT_LOCAL_CLUSTER_NAME,
          CONFLUENT_LOCAL_KAFKAREST_URI,
          CONFLUENT_LOCAL_KAFKAREST_HOSTNAME,
          ""
      );
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterRelated(
      String related
  ) {
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaBrokerListResponse(
      String kind,
      ListMetadata metadata,
      @JsonProperty(required = true) List<KafkaBrokerResponse> data
  ) implements ListResponse<KafkaBrokerResponse, KafkaBrokerResponse> {
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaBrokerResponse(
      String kind,
      JsonNode metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty(value = "broker_id", required = true) String brokerId,
      String host,
      String port,
      String rack,
      @JsonProperty("configs") KafkaClusterRelated config,
      @JsonProperty("partition_replicas") KafkaClusterRelated partitionReplicas
  ) implements ListItem<KafkaBrokerResponse> {

    @Override
    public KafkaBrokerResponse toRepresentation() {
      return this;
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaBrokerConfigResponse(
      String kind,
      JsonNode metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty(value = "broker_id", required = true) String brokerId,
      @JsonProperty(required = true) String name,
      String value,
      @JsonProperty("is_read_only") boolean isReadOnly,
      @JsonProperty("is_sensitive") boolean isSensitive,
      @JsonProperty("is_default") boolean isDefault,
      String source,
      JsonNode synonyms
  ) implements ListItem<KafkaBrokerConfigResponse> {

    @Override
    public KafkaBrokerConfigResponse toRepresentation() {
      return this;
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  protected record SchemaRegistryConfigResponse(
      @JsonProperty(value = "compatibilityLevel", required = true) String compatibilityLevel
  ) {
  }
}
