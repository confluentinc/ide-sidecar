package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Implements Confluent REST Proxy for Kafka's <i>ProduceRequestData</i> class, so that we can make
 * sure that the <i>data</i> field is always included when serializing the
 * {@link ProduceRequestData} to JSON, even when it is <i>null</i>.
 *
 * @see <a href="https://github.com/confluentinc/ide-sidecar/issues/516">ide-sidecar#516</a>
 * @see <a href="https://github.com/confluentinc/vscode/issues/3065">vscode#3065</a>
 * @see <a href="https://github.com/confluentinc/kafka-rest/blob/31952284c970fd764a666e92a519c3569008fabf/kafka-rest/src/main/java/io/confluent/kafkarest/entities/v3/ProduceRequest.java#L164">ProduceRequestData</a>
 */
@RegisterForReflection
public record ProduceRequestData(
    String type,
    String subject,
    @JsonProperty("subject_name_strategy") String subjectNameStrategy,
    @JsonProperty("schema_id") Integer schemaId,
    @JsonProperty("schema_version") Integer schemaVersion,
    String schema,
    @JsonInclude(Include.ALWAYS) Object data
) {
}
