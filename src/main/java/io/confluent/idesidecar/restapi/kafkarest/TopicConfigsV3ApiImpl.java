package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.jetbrains.annotations.NotNull;

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

  @POST
  @Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs:alter")
  @Consumes({ "application/json" })
  @Produces({ "application/json", "text/html" })
  public Uni<Void> updateKafkaTopicConfigBatch(
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @Valid AlterConfigBatchRequestData alterConfigBatchRequestData
  ) {
    return topicConfigsManager.updateKafkaTopicConfigBatch(
        clusterId, topicName, alterConfigBatchRequestData
    );
  }

  private static TopicConfigDataList getTopicConfigDataList(
      String clusterId, String topicName, List<ConfigEntry> configs
  ) {
    return TopicConfigDataList.builder()
        .metadata(getTopicConfigCollectionMetadata(clusterId, topicName))
        .data(configs
            .stream()
            .map(entry -> getTopicConfigData(clusterId, topicName, entry))
            .toList()
        )
        .build();
  }

  private static ResourceCollectionMetadata getTopicConfigCollectionMetadata(String clusterId, String topicName) {
    return ResourceCollectionMetadata.builder()
        // Expose the public Kafka REST Proxy endpoint as the resource URL
        .self("/kafka/v3/clusters/%s/topics/%s/configs".formatted(clusterId, topicName))
        .next(null)
        .build();
  }

  private static TopicConfigData getTopicConfigData(String clusterId, String topicName, ConfigEntry entry) {
    return TopicConfigData.builder()
        .clusterId(clusterId)
        .topicName(topicName)
        .name(entry.name())
        .value(entry.value())
        .isDefault(entry.isDefault())
        .isReadOnly(entry.isReadOnly())
        .isSensitive(entry.isSensitive())
        .source(String.valueOf(entry.source()))
        .synonyms(entry
            .synonyms()
            .stream()
            .map(TopicConfigsV3ApiImpl::getConfigSynonymData)
            .toList()
        ).build();
  }

  private static ConfigSynonymData getConfigSynonymData(ConfigEntry.ConfigSynonym synonym) {
    return ConfigSynonymData.builder()
        .name(synonym.name())
        .value(synonym.value())
        .source(String.valueOf(synonym.source()))
        .build();
  }
}
