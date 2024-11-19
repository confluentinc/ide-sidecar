package io.confluent.idesidecar.restapi.kafkarest;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;

@RecordBuilder
public record TopicConfig (
    String clusterId,
    String topicName,
    String name,
    String value,
    boolean isDefault,
    boolean isReadOnly,
    boolean isSensitive,
    ConfigEntry.ConfigSource source,
    List<ConfigSynonym> synonyms
) implements AbstractConfig {

}
