package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.events.ClusterKind;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.events.ServiceKind;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.util.Crn;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * A {@link CCloudFetcher} that uses the {@link ConnectionStateManager} to find CCloud
 * {@link Connection}s and the CCloud UI to find CCloud resources.
 *
 * <p>This fetcher makes use of
 * <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * so that other components can observe changes in the loaded {@link Cluster} instances. Each event
 * has the following {@link Lifecycle} qualifier:
 * <ul>
 *   <li>{@link Lifecycle.Updated}</li>
 * </ul>
 * and the following {@link ServiceKind} qualifiers:
 * <ul>
 *   <li>{@link ServiceKind.CCloud}</li>
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
public class RealCCloudFetcher extends ConfluentCloudRestClient implements CCloudFetcher {

  private static final String CONFLUENT_CLOUD_ORGS_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.resources.org-list-uri", String.class);

  private static final String CONFLUENT_CLOUD_ENVS_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.resources.env-list-uri", String.class);

  private static final String CONFLUENT_CLOUD_LKCS_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.resources.lkc-list-uri", String.class);

  private static final String CONFLUENT_CLOUD_SRS_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.resources.sr-list-uri", String.class);

  private static final String CONFLUENT_CLOUD_FLINK_COMPUTE_POOLS_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.connections.ccloud.resources.flink-compute-pools-uri", String.class);

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  Event<ClusterEvent> clusterEvents;

  @Override
  public Multi<CCloudConnection> getConnections() {
    return Multi.createFrom().iterable(connectionStateManager
            .getConnectionStates()
            .stream()
            .filter(connection -> ConnectionType.CCLOUD.equals(connection.getType())).toList())
        .map(connection -> new CCloudConnection(
            connection.getSpec().id(), connection.getSpec().name()));
  }

  @Override
  public Uni<CCloudConnection> getConnection(String connectionId) {
    try {
      var spec = connectionStateManager.getConnectionSpec(connectionId);
      if (!ConnectionType.CCLOUD.equals(spec.type())) {
        return Uni.createFrom().failure(
            new ConnectionNotFoundException(
                "Connection %s is not a CCloud connection".formatted(connectionId)));
      } else {
        return Uni.createFrom().item(new CCloudConnection(spec.id(), spec.name()));
      }
    } catch (ConnectionNotFoundException e) {
      return Uni.createFrom().failure(e);
    }
  }

  @Override
  public Multi<CCloudOrganization> getOrganizations(String connectionId) {
    // First get the information about the current organization
    var orgDetails = currentOrgDetails(connectionId);
    // Then fetch all the organizations this user is part of
    var headers = headersFor(connectionId);
    return listItems(headers, CONFLUENT_CLOUD_ORGS_URI, null, this::parseOrgList)
        // and add the connection ID to all organization objects
        .map(org -> org.withConnectionId(connectionId))
        .map(org -> org.withCurrentOrganization(orgDetails));
  }

  @Override
  public Multi<CCloudEnvironment> getEnvironments(String connectionId, PageLimits limits) {
    // First get the information about the current organization
    var orgDetails = currentOrgDetails(connectionId);
    var headers = headersFor(connectionId);
    return listItems(headers, CONFLUENT_CLOUD_ENVS_URI, limits, this::parseEnvList)
        // and add the connection ID to all environment objects
        .map(env -> env.withConnectionId(connectionId))
        .map(env -> env.withCurrentOrganization(orgDetails));
  }

  @Override
  public Multi<CCloudKafkaCluster> getKafkaClusters(
      String connectionId,
      String envId,
      PageLimits limits
  ) {
    var headers = headersFor(connectionId);
    return listItems(
        headers,
        CONFLUENT_CLOUD_LKCS_URI.formatted(envId),
        limits,
        this::parseLkcList
    )
        // and add the connection ID to all cluster objects
        .map(lkc -> lkc.withConnectionId(connectionId))
        // and fire an event that this cluster was loaded
        .map(lkc -> this.onLoad(connectionId, lkc));
  }

  /**
   * Gets the one and only schema registry for the given environment.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param envId        the identifier of the CCloud environment
   * @return the schema registry
   */
  @Override
  public Uni<CCloudSchemaRegistry> getSchemaRegistry(String connectionId, String envId) {
    var headers = headersFor(connectionId);
    return getItem(
        headers,
        CONFLUENT_CLOUD_SRS_URI.formatted(envId),
        this::parseSchemaRegistryList
    )
        .onItem()
        .transformToUni(response -> {
          // Convert the raw response item to the representation
          if (response.data() == null) {
            return Uni.createFrom().nullItem();
          }
          return response.data()
              .stream()
              .map(SchemaRegistryResponse::toRepresentation)
              .map(reg -> reg.withConnectionId(connectionId))
              .map(reg -> this.onLoad(connectionId, reg))
              .findFirst().map(reg -> Uni.createFrom().item(reg))
              .orElseGet(Uni.createFrom()::nullItem);
        });
  }

  protected <ClusterT extends Cluster> ClusterT onLoad(String connectionId, ClusterT cluster) {
    // Fire an event for this cluster
    ClusterEvent.onLoad(
        clusterEvents,
        cluster,
        ConnectionType.CCLOUD,
        connectionId
    );
    return cluster;
  }

  /**
   * Parse the given JSON response containing a list of organizations.
   *
   * @param json  the response payload
   * @param state the pagination state, which should be modified as each page is parsed
   * @return the page of results
   */
  PageOfResults<CCloudOrganization> parseOrgList(String json, PaginationState state) {
    return parseList(json, state, ListOrganizationsResponse.class);
  }

  /**
   * Parse the given JSON response containing a list of environments.
   *
   * @param json  the response payload
   * @param state the pagination state, which should be modified as each page is parsed
   * @return the page of results
   */
  PageOfResults<CCloudEnvironment> parseEnvList(String json, PaginationState state) {
    return parseList(json, state, ListEnvironmentsResponse.class);
  }

  /**
   * Parse the given JSON response containing a list of Kafka clusters.
   *
   * @param json  the response payload
   * @param state the pagination state, which should be modified as each page is parsed
   * @return the page of results
   */
  PageOfResults<CCloudKafkaCluster> parseLkcList(String json, PaginationState state) {
    return parseList(json, state, ListKafkaClustersResponse.class);
  }

  /**
   * Parse the given JSON response containing a single schema registry.
   *
   * @param url  the URL used to get the response payload
   * @param json the response payload
   * @return the parsed response
   */
  SchemaRegistryListResponse parseSchemaRegistryList(String url, String json) {
    return parseRawItem(url, json, SchemaRegistryListResponse.class);
  }

  static CCloudGovernancePackage parsePackage(GovernanceConfig value) {
    if (value != null) {
      try {
        return CCloudGovernancePackage.valueOf(value.packageName().toUpperCase());
      } catch (IllegalArgumentException e) {
        return CCloudGovernancePackage.NONE;
      }
    }
    return CCloudGovernancePackage.NONE;
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ListOrganizationsResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      ListMetadata metadata,
      @JsonProperty(value = "data", required = true) List<OrganizationResponse> data
  ) implements ListResponse<OrganizationResponse, CCloudOrganization> {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record OrganizationResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      @JsonProperty(required = true) String id,
      JsonNode metadata,
      @JsonProperty(value = "display_name") String displayName,
      @JsonProperty(value = "jit_enabled") Boolean jitEnabled
  ) implements ListItem<CCloudOrganization> {

    @Override
    public CCloudOrganization toRepresentation() {
      return new CCloudOrganization(
          id,
          displayName,
          false // TODO: the connection should know the current org
      );
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ListEnvironmentsResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      ListMetadata metadata,
      @JsonProperty(value = "data", required = true) List<EnvironmentResponse> data
  ) implements ListResponse<EnvironmentResponse, CCloudEnvironment> {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record EnvironmentResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      @JsonProperty(required = true) String id,
      EnvironmentMetadata metadata,
      @JsonProperty(value = "display_name") String displayName,
      @JsonProperty(value = "jit_enabled") Boolean jitEnabled,
      @JsonProperty(value = "stream_governance_config") GovernanceConfig governance
  ) implements ListItem<CCloudEnvironment> {

    @Override
    public CCloudEnvironment toRepresentation() {
      return new CCloudEnvironment(
          id,
          displayName,
          parseOrganization(metadata.resourceName),
          parsePackage(governance)
      );
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record EnvironmentMetadata(
      @JsonProperty(value = "created_at") String createdAt,
      @JsonProperty(value = "resource_name") String resourceName,
      String self,
      @JsonProperty(value = "updated_at") String updatedAt
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record GovernanceConfig(
      @JsonProperty(value = "package") String packageName
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ListKafkaClustersResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      ListMetadata metadata,
      @JsonProperty(value = "data", required = true) List<KafkaClusterResponse> data
  ) implements ListResponse<KafkaClusterResponse, CCloudKafkaCluster> {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record KafkaClusterResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      @JsonProperty(required = true) String id,
      JsonNode metadata,
      @JsonProperty(value = "spec") KafkaClusterSpec spec,
      @JsonProperty(value = "status") KafkaClusterStatus status
  ) implements ListItem<CCloudKafkaCluster> {

    @Override
    public CCloudKafkaCluster toRepresentation() {
      if (spec == null) {
        return new CCloudKafkaCluster(
            id,
            "",
            CloudProvider.NONE,
            "",
            "",
            "",
            null,
            null,
            null
        );
      } else {
        return new CCloudKafkaCluster(
            id,
            spec.displayName,
            CloudProvider.of(spec.cloud),
            spec.region,
            spec.httpEndpoint,
            spec.bootstrapEndpoint,
            parseOrganization(spec.environment.resourceName),
            spec.environmentReference(),
            null
        );
      }
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterSpec(
      @JsonProperty(value = "display_name") String displayName,
      String availability,
      String cloud,
      String region,
      JsonNode config,
      @JsonProperty(value = "kafka_bootstrap_endpoint") String bootstrapEndpoint,
      @JsonProperty(value = "http_endpoint") String httpEndpoint,
      @JsonProperty(value = "api_endpoint") String apiEndpoint,
      EnvironmentReference environment,
      NetworkReference network,
      KafkaClusterByok byok
  ) {

    public CCloudReference environmentReference() {
      return environment == null ? null : new CCloudReference(environment.id, null);
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterStatus(
      String phase,
      Integer cku
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record EnvironmentReference(
      String id,
      String environment,
      String related,
      @JsonProperty(value = "resource_name") String resourceName
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record NetworkReference(
      String id,
      String environment,
      String related,
      @JsonProperty(value = "resource_name") String resourceName
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record KafkaClusterByok(
      String id,
      String related,
      @JsonProperty(value = "resource_name") String resourceName
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record SchemaRegistryListResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      ListMetadata metadata,
      @JsonProperty(value = "data", required = true) List<SchemaRegistryResponse> data
  ) implements ListResponse<SchemaRegistryResponse, CCloudSchemaRegistry> {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record SchemaRegistryResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      @JsonProperty(required = true) String id,
      JsonNode metadata,
      @JsonProperty(value = "spec") SchemaRegistrySpec spec,
      @JsonProperty(value = "status") SchemaRegistryStatus status
  ) implements ListItem<CCloudSchemaRegistry> {

    @Override
    public CCloudSchemaRegistry toRepresentation() {
      if (spec == null) {
        return new CCloudSchemaRegistry(
            id,
            "",
            CloudProvider.NONE,
            "",
            null,
            null,
            null
        );
      }
      return new CCloudSchemaRegistry(
          id,
          spec.httpEndpoint,
          CloudProvider.of(spec.cloud),
          spec.region,
          parseOrganization(spec.environment.resourceName),
          spec.environmentReference(),
          null
      );
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record SchemaRegistrySpec(
      @JsonProperty(value = "display_name") String displayName,
      EnvironmentReference environment,
      @JsonProperty(value = "http_endpoint") String httpEndpoint,
      String cloud,
      String region,
      @JsonProperty(value = "package") String governancePackage
  ) {

    public CCloudReference environmentReference() {
      return environment == null ? null : new CCloudReference(environment.id, null);
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  record SchemaRegistryStatus(
      String phase
  ) {

  }

  /**
   * Fetch organization id from CRN in the form:
   * crn://confluent.cloud/organization=23b1185e-d874-4f61-81d6-c9c61aa8969c/environment=env-123
   */
  private static CCloudReference parseOrganization(String crnString) {
    var crn = Crn.fromString(crnString);
    var organization = crn.elements().stream()
        .filter(e -> e.resourceType().equals("organization"))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Organization not found in CRN"));
    return new CCloudReference(
        organization.resourceName(),
        // We cannot determine the name of the org, so set it to null
        null
    );
  }

  protected CCloudOAuthContext.OrganizationDetails currentOrgDetails(String connectionId) {
    try {
      var state = connectionStateManager.getConnectionState(connectionId);
      if (state instanceof CCloudConnectionState ccloudState) {
        return ccloudState.getOauthContext().getCurrentOrganization();
      }
    } catch (ConnectionNotFoundException e) {
      // do nothing
    }
    return null;
  }

  public Multi<CCloudFlinkComputePool> getFlinkComputePools(
      String connectionId,
      String envId
  ) {
    var headers = headersFor(connectionId);
    String url = CONFLUENT_CLOUD_FLINK_COMPUTE_POOLS_URI.formatted(envId);
    Log.infof("Fetching Flink compute pools from URL: %s with headers: %s", url, headers);
    return listItems(headers, url, null, this::parseFlinkComputePoolsList)
        .map(pool -> pool.withConnectionId(connectionId));
  }

  private PageOfResults<CCloudFlinkComputePool> parseFlinkComputePoolsList(
      String json,
      PaginationState state
  ) {
    return parseList(json, state, ListFlinkComputePoolsResponse.class);
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ListFlinkComputePoolsResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      ListMetadata metadata,
      @JsonProperty(value = "data", required = true) List<FlinkComputePoolResponse> data
  ) implements ListResponse<FlinkComputePoolResponse, CCloudFlinkComputePool> {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record FlinkComputePoolResponse(
      @JsonProperty(value = "api_version") String apiVersion,
      String kind,
      @JsonProperty(required = true) String id,
      JsonNode metadata,
      @JsonProperty(value="environment") CCloudReference environment,
      @JsonProperty(value="organization") CCloudReference organization,
      @JsonProperty(value = "spec") FlinkComputePoolSpec spec,
      @JsonProperty(value = "status") FlinkComputePoolStatus status
      ) implements ListItem<CCloudFlinkComputePool> {
    @Override
    public CCloudFlinkComputePool toRepresentation() {
      return new CCloudFlinkComputePool(
          id,
          spec.displayName,
          spec.provider,
          spec.region,
          spec.maxCfu,
          environment,
          organization,
          null
      );
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record FlinkComputePoolSpec(
      @JsonProperty(value = "display_name") String displayName,
      @JsonProperty(value = "cloud") String provider,
      @JsonProperty(value = "region") String region,
      @JsonProperty(value = "max_cfu") int maxCfu,
      @JsonProperty(value = "description") String description
  ) {
  }
  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record FlinkComputePoolStatus(
      @JsonProperty(value = "phase") String phase,
      @JsonProperty(value = "current_cfu") int currentCfu
  ) {
  }


}
