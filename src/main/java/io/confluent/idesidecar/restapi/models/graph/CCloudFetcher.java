package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PageLimits;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * The provider of CCloud resource objects, including {@link CCloudOrganization},
 * {@link CCloudEnvironment}, {@link CCloudKafkaCluster} and {@link CCloudSchemaRegistry} objects.
 * These methods are called to load the CCloud resources with the given relationship to the
 * identified parent resource.
 *
 * <p>A {@link CCloudConnection} is a connection to CCloud using a single principal, and
 * has an identifier unique to this process.
 */
@RegisterForReflection
public interface CCloudFetcher {

  /**
   * Get the Flink compute pools accessible to the specified CCloud connection's principal and within the specified environment.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param envId        the identifier of the CCloud environment
   * @return the list of Flink compute pools; may be empty
   */
  Multi<FlinkComputePool> getFlinkComputePools(String connectionId, String envId);
  Multi<FlinkComputePool> listAllFlinkComputePools();
  /**
   * Get all CCloud connections.
   *
   * @return the list of all CCloud connections; never null but possibly empty
   */
  Multi<CCloudConnection> getConnections();

  /**
   * Get the CCloud connection with the specified identifier.
   *
   * @param connectionId the identifier of the CCloud connection
   * @return the CCloud connection
   */
  Uni<CCloudConnection> getConnection(String connectionId);

  /**
   * Get the organizations to which the authenticated principal associated from the identified
   * CCloud connection belongs. One of the organizations will be designated as
   * {@link CCloudOrganization#current() current} (defined during authentication).
   *
   * @param connectionId the identifier of the CCloud connection
   * @return the list of CCloud organizations to which the principal belongs
   */
  Multi<CCloudOrganization> getOrganizations(
      String connectionId
  );

  /**
   * Get the CCloud environments for the {@link #getOrganizations current organization} accessible
   * to the specified CCloud connection's principal.
   *
   * <p>Each environment describes the
   * {@link CCloudEnvironment#governancePackage() governance package} that is used; if the
   * {@link CCloudGovernancePackage#ESSENTIALS} or {@link CCloudGovernancePackage#ADVANCED} packages
   * are used, then the {@link #getSchemaRegistry(String, String)} method can be used to obtain the
   * information about the environment's {@link CCloudSchemaRegistry} instance.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param limits       the limits on the stream of results; may be null
   * @return the list of CCloud environments within the current organization; may be empty
   */
  Multi<CCloudEnvironment> getEnvironments(
      String connectionId,
      PageLimits limits
  );

  /**
   * Get the information about the CCloud Schema Registry associated with the specified
   * environment.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param envId        the identifier of the CCloud environment
   * @return the Schema Registry; null if the environment has no governance package.
   */
  Uni<CCloudSchemaRegistry> getSchemaRegistry(
      String connectionId,
      String envId
  );

  /**
   * Get the information about the CCloud Kafka clusters accessible to the specified CCloud
   * connection's principal and within the specified environment.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param envId        the identifier of the CCloud environment
   * @param limits       the limits on the stream of results; may be null
   * @return the list of Kafka clusters; may be empty
   */
  Multi<CCloudKafkaCluster> getKafkaClusters(
      String connectionId,
      String envId,
      PageLimits limits
  );

  /**
   * Get the information about the CCloud Kafka clusters accessible to the specified CCloud
   * connection's principal and within the specified environment.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param envId        the identifier of the CCloud environment
   * @param criteria     the search criteria
   * @param limits       the limits on the stream of results; may be null
   * @return the list of Kafka clusters; may be empty
   */
  default Multi<CCloudKafkaCluster> getKafkaClusters(
      String connectionId,
      String envId,
      CCloudSearchCriteria criteria,
      PageLimits limits
  ) {
    return getKafkaClusters(connectionId, envId, limits).filter(lkc -> lkc.matches(criteria));
  }

  /**
   * Find all the Kafka clusters accessible to the specified CCloud connection's principal that
   * satisfy the specified search criteria.
   *
   * <p>By default, this method gets all environments, then gets call clusters for each
   * environment, and applies the criteria to find all matching clusters.
   *
   * @param connectionId  the identifier of the CCloud connection
   * @param criteria      the search criteria
   * @param envLimits     the limits on the environments; may be null
   * @param clusterLimits the limits on the clusters; may be null
   * @return the list of Kafka clusters; may be empty
   */
  default Multi<CCloudKafkaCluster> findKafkaClusters(
      String connectionId,
      CCloudSearchCriteria criteria,
      PageLimits envLimits,
      PageLimits clusterLimits
  ) {
    return this
        .getEnvironments(connectionId, envLimits)
        .flatMap(env -> getKafkaClusters(connectionId, env.id(), criteria, clusterLimits));
  }

  /**
   * Find the Kafka cluster accessible to the specified CCloud connection's principal that has the
   * given resource ID.
   *
   * <p>By default, this method gets all environments, then gets call clusters for each
   * environment, and applies the criteria to find the first Kafka cluster with the given ID.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param lkcId        the identifier of the Kafka cluster
   * @return the Kafka cluster; never null but possibly contains null
   */
  default Uni<CCloudKafkaCluster> findKafkaCluster(
      String connectionId,
      String lkcId
  ) {
    return findKafkaClusters(
        connectionId,
        CCloudSearchCriteria.create().withResourceId(lkcId),
        PageLimits.DEFAULT,
        PageLimits.DEFAULT
    ).select().first().toUni();
  }

  /**
   * Find the Schema Registry cluster accessible to the specified CCloud connection's principal that
   * has the given resource ID.
   *
   * <p>By default, this method gets all environments, then gets call clusters for each
   * environment, and applies the criteria to find the first Kafka cluster with the given ID.
   *
   * @param connectionId the identifier of the CCloud connection
   * @param lsrcId       the identifier of the Schema Registry cluster
   * @return the Kafka cluster; never null but possibly contains null
   */
  default Uni<CCloudSchemaRegistry> findSchemaRegistry(
      String connectionId,
      String lsrcId
  ) {
    return this
        .getEnvironments(connectionId, PageLimits.DEFAULT)
        .onItem()
        .transformToUniAndMerge(env -> getSchemaRegistry(connectionId, env.id()))
        .select().when(reg -> Uni.createFrom().item(reg.id().equalsIgnoreCase(lsrcId)))
        .collect()
        .first();
  }
}