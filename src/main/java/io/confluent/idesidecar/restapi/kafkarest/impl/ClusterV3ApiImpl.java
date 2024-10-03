package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
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
    return uniStage(adminClient.describeCluster().clusterId().toCompletionStage())
        .chain(cid -> {
          if (!cid.equals(clusterId)) {
            return Uni.createFrom().failure(new NotFoundException("Cluster not found"));
          } else {
            return uniItem(ClusterData
                .builder()
                .kind("KafkaCluster")
                .clusterId(cid)
                .build()
            );
          }
        });
  }

  @Override
  public Uni<ClusterDataList> listKafkaClusters() {
    return uniStage(adminClient.describeCluster().clusterId().toCompletionStage())
        .chain(cid -> uniItem(ClusterDataList
                .builder()
                .kind("KafkaClusterList")
                .data(List.of(ClusterData
                    .builder()
                    .kind("KafkaCluster")
                    .clusterId(cid)
                    .build()))
                .build()));
  }
}
