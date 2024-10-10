package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

@RequestScoped
public class ClusterV3ApiImpl implements ClusterV3Api {

  @Inject
  ClusterManagerImpl clusterManager;

  @Override
  public Uni<ClusterData> getKafkaCluster(String clusterId) {
    return clusterManager.getKafkaCluster(clusterId);
  }

  @Override
  public Uni<ClusterDataList> listKafkaClusters() {
    return clusterManager.listKafkaClusters();
  }
}
