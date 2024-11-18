package io.confluent.idesidecar.restapi.kafkarest;


import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdateConfigRequestData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import java.util.List;

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

//
//  @Override
//  public Uni<Void> updateKafkaTopicConfigBatch(String clusterId, String topicName, AlterConfigBatchRequestData alterConfigBatchRequestData) {
//    return configManager.alterConfigs(
//        clusterId,
//        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
//        // Convert the AlterConfigBatchRequestData to a list of AlterConfigCommands
//
//    );
//  }

}
