package io.confluent.idesidecar.restapi.kafkarest.controllers;

import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.smallrye.mutiny.Uni;

public interface ClusterManager {

  Uni<ClusterData> getKafkaCluster(String clusterId);

  Uni<ClusterDataList> listKafkaClusters();
}
