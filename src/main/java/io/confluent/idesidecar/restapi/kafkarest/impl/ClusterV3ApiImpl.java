package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forAcls;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forAllPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forBrokerConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forBrokers;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forClusters;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forConsumerGroups;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forController;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.getClusterCrn;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;

@RequestScoped
public class ClusterV3ApiImpl implements ClusterV3Api {

  @Inject
  AdminClient adminClient;

  @Override
  public Uni<ClusterData> getKafkaCluster(String clusterId) {
    return describeCluster()
        .chain(cluster -> {
          if (!cluster.id().equals(clusterId)) {
            return Uni.createFrom().failure(new NotFoundException("Cluster not found"));
          } else {
            return uniItem(fromClusterId(cluster));
          }
        });
  }

  record ClusterDescribe(String id, int controllerId) {
  }

  @Override
  public Uni<ClusterDataList> listKafkaClusters() {
    return describeCluster()
        .chain(cluster -> uniItem(ClusterDataList
            .builder()
            .metadata(ResourceCollectionMetadata
                .builder()
                .self(forClusters().getRelated())
                // We don't support pagination
                .next(null)
                .build()
            )
            .kind("KafkaClusterList")
            .data(List.of(fromClusterId(cluster)))
            .build())
        );
  }

  private Uni<ClusterDescribe> describeCluster() {
    return uniStage(adminClient.describeCluster().clusterId().toCompletionStage())
        .chain(cid ->
            uniStage(
                adminClient.describeCluster().controller().toCompletionStage()
            ).map(controllerNode -> new ClusterDescribe(cid, controllerNode.id()))
        );
  }

  private ClusterData fromClusterId(ClusterDescribe cluster) {
    return ClusterData
        .builder()
        .kind("KafkaCluster")
        .metadata(ResourceMetadata
            .builder()
            .self(getClusterCrn(cluster.id()).toString())
            .resourceName(cluster.id())
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
}
