package io.confluent.idesidecar.restapi.kafkarest;


import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import java.util.List;
import java.util.Optional;

/**
 * RequestScoped bean for managing Kafka clusters. Creating the bean as {@link RequestScoped} allows
 * us to inject the {@link HttpServerRequest} which is used to get the connection ID from the
 * request headers.
 */
@RequestScoped
public class TopicConfigManagerImpl implements TopicConfigManager {

  @Inject
  ConfigManager configManager;

  @Override
  public Uni<List<ConfigEntry>> listKafkaTopicConfigs(String clusterId, String topicName) {
    return configManager.listConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    );
  }

  @Override
  public Uni<Void> updateKafkaTopicConfigBatch(
      String clusterId,
      String topicName,
      AlterConfigBatchRequestData alterConfigBatchRequestData
  ) {
    // Check if request is empty
    if (alterConfigBatchRequestData.getData().isEmpty()) {
      throw new BadRequestException("No configurations provided");
    }

    return configManager.alterConfigs(
        clusterId,
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        alterConfigBatchRequestData
            .getData()
            .stream()
            .map(AlterConfigCommand::fromAlterConfigRequestDataInner)
            .toList(),
        Optional.ofNullable(alterConfigBatchRequestData.getValidateOnly()).orElse(false)
    );
  }
}
