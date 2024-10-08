package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Api;
import io.confluent.idesidecar.restapi.kafkarest.controllers.AdminClientService;
import io.confluent.idesidecar.restapi.kafkarest.controllers.ClusterManagerImpl;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import org.apache.kafka.clients.admin.AdminClient;

@RequestScoped
public class ClusterV3ApiImpl implements ClusterV3Api {

  @HeaderParam(CONNECTION_ID_HEADER)
  private String connectionId;

  @Inject
  AdminClientService adminClientService;

  @Override
  public Uni<ClusterData> getKafkaCluster(String clusterId) {
      return new ClusterManagerImpl(adminClientService.getAdminClientConfig(connectionId)).getKafkaCluster(clusterId);

  }

  @Override
  public Uni<ClusterDataList> listKafkaClusters() {
      return new ClusterManagerImpl(adminClientService.getAdminClientConfig(connectionId)).listKafkaClusters();

  }
}
