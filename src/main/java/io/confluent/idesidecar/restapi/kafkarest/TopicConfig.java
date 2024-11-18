package io.confluent.idesidecar.restapi.kafkarest;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;

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
    ConfigSource source,
    List<ConfigSynonym> synonyms
) implements AbstractConfig {
}
