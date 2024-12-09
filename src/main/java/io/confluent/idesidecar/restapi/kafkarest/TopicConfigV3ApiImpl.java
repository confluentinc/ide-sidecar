package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConfigSynonymData;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicConfigData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicConfigDataList;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniOnItemIgnore;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * Resource for managing Kafka topic configurations.
 */
@RequestScoped
@Path("/internal/kafka/v3/clusters/{cluster_id}/topics")
public class TopicConfigV3ApiImpl {

  @Inject
  TopicManager topicManager;

  @Inject
  TopicConfigManager topicConfigsManager;

  @GET
  @Path("{topic_name}/configs")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
  public Uni<TopicConfigDataList> listKafkaTopicConfigs(
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName
  ) {
    return ensureTopicExists(clusterId, topicName)
        .andSwitchTo(() -> topicConfigsManager
            .listKafkaTopicConfigs(clusterId, topicName)
            .onItem()
            .transform(configs -> getTopicConfigDataList(clusterId, topicName, configs))
        );
  }

  @POST
  @Path("{topic_name}/configs:alter")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
  public Uni<Void> updateKafkaTopicConfigBatch(
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @Valid AlterConfigBatchRequestData alterConfigBatchRequestData
  ) {
    return ensureTopicExists(clusterId, topicName)
        .andSwitchTo(() -> topicConfigsManager
            .updateKafkaTopicConfigBatch(
                clusterId, topicName, alterConfigBatchRequestData)
        );
  }

  private UniOnItemIgnore<TopicDescription> ensureTopicExists(String clusterId, String topicName) {
    return topicManager
        .getKafkaTopic(clusterId, topicName, false)
        .onItem()
        .ignore();
  }

  private static TopicConfigDataList getTopicConfigDataList(
      String clusterId, String topicName, List<ConfigEntry> configs
  ) {
    return TopicConfigDataList
        .builder()
        .kind("KafkaTopicConfigList")
        .metadata(getTopicConfigCollectionMetadata(clusterId, topicName))
        .data(configs
            .stream()
            .map(entry -> getTopicConfigData(clusterId, topicName, entry))
            .toList()
        )
        .build();
  }

  private static ResourceCollectionMetadata getTopicConfigCollectionMetadata(
      String clusterId, String topicName
  ) {
    return ResourceCollectionMetadata.builder()
        // Expose the public Kafka REST Proxy endpoint as the resource URL
        .self("/kafka/v3/clusters/%s/topics/%s/configs".formatted(clusterId, topicName))
        .next(null)
        .build();
  }

  private static TopicConfigData getTopicConfigData(
      String clusterId, String topicName, ConfigEntry entry
  ) {
    return TopicConfigData
        .builder()
        .kind("KafkaTopicConfig")
        .metadata(ResourceMetadata
            .builder()
            .self(
                // Note: We don't yet implement Get Topic Config, we can add later if needed
                "/kafka/v3/clusters/%s/topics/%s/configs/%s"
                    .formatted(clusterId, topicName, entry.name())
            )
            .resourceName(null)
            .build()
        )
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
            .map(TopicConfigV3ApiImpl::getConfigSynonymData)
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
