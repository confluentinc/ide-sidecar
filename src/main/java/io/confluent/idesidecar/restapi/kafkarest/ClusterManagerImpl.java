package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forAcls;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forAllPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forBrokerConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forBrokers;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forCluster;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forClusters;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerGroups;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forController;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.cache.AdminClients;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.cache.ClusterCache.ClusterInfo;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

/**
 * RequestScoped bean for managing Kafka clusters. Creating the bean as {@link RequestScoped} allows
 * us to inject the {@link HttpServerRequest} which is used to get the connection ID from the
 * request headers.
 */
@RequestScoped
public class ClusterManagerImpl implements ClusterManager {

  @Inject
  AdminClients adminClients;

  @Inject
  ClusterCache clusterCache;

  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  @Override
  public Uni<ClusterData> getKafkaCluster(String clusterId) {
    return describeCluster(clusterId)
        .chain(cid -> {
          if (!cid.id().equals(clusterId)) {
            return Uni.createFrom().failure(new ClusterNotFoundException(
                "Kafka cluster '%s' not found.".formatted(clusterId)
            ));
          }
          return uniItem(cid);
        })
        .map(this::fromClusterId);
  }

  @Override
  public Uni<ClusterDataList> listKafkaClusters() {
    return uniItem(
        // Get the first Kafka cluster for the connection
        (Supplier<Optional<ClusterInfo<KafkaCluster>>>)
            () -> clusterCache.getKafkaClusterForConnection(connectionId.get()))
        // Run the supplier on the default worker pool since it may block
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(Supplier::get)
        // Call describeCluster if the cluster info is present
        // else return null
        .chain(clusterInfo -> clusterInfo
            .map(kafkaClusterInfo -> this.describeCluster(kafkaClusterInfo.spec().id()))
            .orElse(Uni.createFrom().nullItem())
        )
        // Call describeCluster on each cluster ID
        .chain(clusterId -> uniItem(getClusterDataList(clusterId)));
  }

  private ClusterDataList getClusterDataList(ClusterDescribe cluster) {
    return ClusterDataList
        .builder()
        .metadata(ResourceCollectionMetadata
            .builder()
            .self(forClusters().getRelated())
            // We don't support pagination
            .next(null)
            .build()
        )
        .kind("KafkaClusterList")
        .data(Optional
            .ofNullable(cluster)
            .stream()
            .map(this::fromClusterId)
            .collect(Collectors.toList())
        ).build();
  }

  private Uni<ClusterDescribe> describeCluster(String clusterId) {
    return uniItem((Supplier<AdminClient>)
        () -> adminClients.getClient(connectionId.get(), clusterId))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(supplier -> supplier.get().describeCluster())
        .chain(describeClusterResult ->
            uniStage(describeClusterResult.clusterId().toCompletionStage())
                .map(id -> new ClusterDescribe(describeClusterResult).withId(id)))
        .chain(cid -> uniStage(cid.result.controller().toCompletionStage()).map(Node::id)
            .map(cid::withControllerId)
        ).chain(cid -> uniStage(cid.result.nodes().toCompletionStage())
            .map(cid::withNodes)
        );
  }

  private ClusterData fromClusterId(ClusterDescribe cluster) {
    return ClusterData
        .builder()
        .kind("KafkaCluster")
        .metadata(ResourceMetadata
            .builder()
            .self(forCluster(cluster.id()).toString())
            // TODO: Construct resource name based on the connection/cluster type
            .resourceName(null)
            .build()
        )
        .clusterId(cluster.id())
        .acls(forAcls(cluster.id()))
        .brokerConfigs(forBrokerConfigs(cluster.id()))
        .brokers(forBrokers(cluster.id()))
        .controller(forController(cluster.id(), cluster.controllerId()))
        .consumerGroups(forConsumerGroups(cluster.id()))
        .topics(forTopics(cluster.id()))
        .partitionReassignments(forAllPartitionReassignments(cluster.id()))
        .build();
  }

  /**
   * Record to hold the KafkaFuture results of the describeCluster operation.
   * Used to pass the results between the various stages of the Uni chain.
   */
  private record ClusterDescribe(
      DescribeClusterResult result,
      String id,
      Integer controllerId,
      Collection<Node> nodes
  ) {
    ClusterDescribe(DescribeClusterResult result) {
      this(result, null, null, null);
    }

    ClusterDescribe withId(String id) {
      return new ClusterDescribe(result, id, controllerId, nodes);
    }

    ClusterDescribe withControllerId(Integer controllerId) {
      return new ClusterDescribe(result, id, controllerId, nodes);
    }

    ClusterDescribe withNodes(Collection<Node> nodes) {
      return new ClusterDescribe(result, id, controllerId, nodes);
    }
  }
}
