package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.models.graph.CCloudConnection;
import io.confluent.idesidecar.restapi.models.graph.CCloudEnvironment;
import io.confluent.idesidecar.restapi.models.graph.CCloudKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.CCloudOrganization;
import io.confluent.idesidecar.restapi.models.graph.CCloudSchemaRegistry;
import io.confluent.idesidecar.restapi.models.graph.CCloudSearchCriteria;
import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PageLimits;
import io.confluent.idesidecar.restapi.models.graph.RealCCloudFetcher;
import io.quarkus.logging.Log;
import io.smallrye.graphql.api.Nullable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.graphql.DefaultValue;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Source;

/**
 * GraphQL API for querying Confluent Cloud resources.
 */
@GraphQLApi
public class ConfluentCloudQueryResource {

  @Inject
  RealCCloudFetcher ccloud;

  public static final PageLimits DEFAULT_LIMITS = PageLimits.DEFAULT;

  /**
   * Get all {@link CCloudConnection}s.
   *
   * @return the connections; never null but possibly empty
   */
  @Query("ccloudConnections")
  @Description("Get all CCloud connections")
  @NonNull
  public Uni<List<CCloudConnection>> getCCloudConnections() {
    return ccloud.getConnections().collect().asList();
  }

  /**
   * Get a specific {@link CCloudConnection} with the given identifier.
   *
   * @param id the identifier
   * @return the connection, or null if there is no such connection with that identifier
   */
  @Query("ccloudConnectionById")
  @Description("Get a CCloud connection with a particular ID")
  public Uni<CCloudConnection> getCCloudConnection(@NonNull String id) {
    Log.infof("Get CCloud connection by id=%s", id);
    return ccloud.getConnection(id);
  }

  /**
   * Get the CCloud organizations to which belongs the user of the given connection to CCloud.
   *
   * @param connection the CCloud connection
   * @return the organizations; never null but may be empty
   */
  @NonNull
  public Uni<List<CCloudOrganization>> getOrganizations(@Source CCloudConnection connection) {
    Log.infof("Get CCloud orgs for connection %s", connection.getId());
    return multiToUni(
        ccloud.getOrganizations(connection.getId())
    );
  }

  /**
   * Get the environments accessible to the given connection to CCloud.
   *
   * @param connection the CCloud connection
   * @return the environments; never null but may be empty
   */
  @NonNull
  public Uni<List<CCloudEnvironment>> getEnvironments(@Source CCloudConnection connection) {
    Log.infof("Get CCloud environments for connection=%s", connection.getId());
    return multiToUni(
        ccloud.getEnvironments(connection.getId(), DEFAULT_LIMITS)
    );
  }

  /**
   * Get the Kafka clusters within the specified {@link CCloudConnection}.
   *
   * @param env the environment
   * @return the Kafka clusters; never null but may be empty
   */
  @NonNull
  public Uni<List<CCloudKafkaCluster>> getKafkaClusters(
      @Source CCloudEnvironment env
  ) {
    Log.infof(
        "Get CCloud kafka clusters for connection %s and %s",
        env.connectionId(),
        env.id()
    );
    return multiToUni(
        ccloud
            .getKafkaClusters(env.connectionId(), env.id(), DEFAULT_LIMITS)
            .map(cluster -> cluster.withEnvironment(env))
            .map(cluster -> cluster.withOrganization(env.organization()))
    );
  }

  /**
   * Get the schema registry for a specified {@link CCloudConnection}. Not all environments have a
   * schema registry, though most do and newer ones do by default.
   *
   * @param env the environment
   * @return the schema registry information; may be null
   */
  @Nullable
  public Uni<CCloudSchemaRegistry> getSchemaRegistry(
      @Source CCloudEnvironment env
  ) {
    Log.infof(
        "Get CCloud schema registry for connection %s and %s",
        env.connectionId(),
        env.id()
    );
    return ccloud.getSchemaRegistry(env.connectionId(), env.id())
        .onItem().ifNotNull().transform(sr ->
            sr.withEnvironment(env).withOrganization(env.organization())
        );
  }

  /**
   * Find the Kafka clusters accessible via the {@link CCloudConnection} with the specified ID whose
   * environment ID, cloud provider name, cloud region, and cluster name match the given criteria.
   * All criteria must be satisfied for there to be a match.
   *
   * @param connectionId  the ID of the connection to use
   * @param environmentId the substring that must be contained in a Kafka cluster's environment ID,
   *                      defaults to a blank string that matches all environment IDs
   * @param provider      the substring that must be contained in a Kafka cluster's cloud provider,
   *                      defaults to a blank string that matches all providers
   * @param region        the substring that must be contained in a Kafka cluster's cloud region,
   *                      defaults to a blank string that matches all regions
   * @param name          the substring that must be contained in a Kafka cluster's name, defaults
   *                      to a blank string that matches all Kafka cluster names
   * @return the Kafka clusters that match any of the criteria; never null but possibly empty
   */
  @Query("findCCloudKafkaClusters")
  @Description("Find CCloud Kafka clusters using a connection and various criteria")
  @NonNull
  public Uni<List<CCloudKafkaCluster>> findKafkaClusters(
      @NonNull String connectionId,
      @DefaultValue("") String environmentId,
      @DefaultValue("") String provider,
      @DefaultValue("") String region,
      @DefaultValue("") String name
  ) {
    Log.infof(
        "Find CCloud kafka clusters for connection %s, name=%s, env=%s, provider=%s, region=%s",
        connectionId, name, environmentId, provider, region
    );
    return multiToUni(
        ccloud.findKafkaClusters(
            connectionId,
            CCloudSearchCriteria.create()
                .withNameContaining(name)
                .withEnvironmentIdContaining(environmentId)
                .withCloudProviderContaining(provider)
                .withRegionContaining(region),
            DEFAULT_LIMITS,
            DEFAULT_LIMITS
        )
    );
  }

  /**
   * Apparently the Smallrye GraphQL plugin does not handle {@link Multi}, so we have to convert it
   * to {@link Uni}.
   *
   * @param multi the Multi&lt;T> stream
   * @param <T>   the type of item
   * @return the Uni&llt;List&lt;T>>
   */
  protected static <T> Uni<List<T>> multiToUni(Multi<T> multi) {
    return multi.collect().asList();
  }
}