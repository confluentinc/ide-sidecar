package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;

/**
 * Resource for managing Kafka topic configurations.
 */
@RequestScoped
public class TopicConfigsV3ApiImpl {

  @Inject
  TopicConfigManager topicConfigsManager;

  @GET
  @Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs")
  @Produces({ "application/json", "text/html" })
  public Uni<TopicConfigDataList> listKafkaTopicConfigs(
      @PathParam("cluster_id")
      String clusterId,@PathParam("topic_name") String topicName
  ) {
    return topicConfigsManager.listKafkaTopicConfigs(clusterId, topicName)
        .onItem()
        .transform(configs -> getTopicConfigDataList(clusterId, topicName, configs));
  }

  @PUT
  @Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs/{name}")
  @Consumes({ "application/json" })
  @Produces({ "application/json", "text/html" })
  public Uni<Void> updateKafkaTopicConfig(
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @PathParam("name") String name,
      @Valid UpdateConfigRequestData updateConfigRequestData
  ) {
    return Uni.createFrom().voidItem();
  }

  @POST
  @Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter")
  @Consumes({ "application/json" })
  @Produces({ "application/json", "text/html" })
  public Uni<Void> updateKafkaTopicConfigBatch(
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @Valid AlterConfigBatchRequestData alterConfigBatchRequestData
  ) {
    return Uni.createFrom().voidItem();
  }


  private static TopicConfigDataList getTopicConfigDataList(
      String clusterId, String topicName, List<ConfigEntry> configs
  ) {
    return TopicConfigDataList.builder()
        .metadata(ResourceCollectionMetadata.builder()
            .self("/v3/clusters/%s/topics/%s/configs".formatted(clusterId, topicName))
            .next(null)
            .build()
        )
//        .data(configs
//            .stream()
//            .map(entry -> TopicConfig.builder()
//                .clusterId(clusterId)
//                .topicName(topicName)
//                .name(entry.name())
//                .value(entry.value())
//                .isDefault(entry.isDefault())
//                .isReadOnly(entry.isReadOnly())
//                .isSensitive(entry.isSensitive())
//                .source(entry.source())
//                .synonyms(entry.synonyms())
//                .build()
//            )
//            .toList()
//        )
        .build();
  }
}
