package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestHeader;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.Valid;
import java.util.Date;
import java.util.List;

/**
 * Implements Confluent REST Proxy for Kafka's <i>ProduceRequest</i> model, so that we can use the
 * ide-sidecar's custom {@link ProduceRequestData} and make sure that the <i>data</i> field is
 * always included when serializing the {@link ProduceRequestData} to JSON, even when it is
 * <i>null</i>.
 *
 * @see <a href="https://github.com/confluentinc/ide-sidecar/issues/516">ide-sidecar#516</a>
 * @see <a href="https://github.com/confluentinc/vscode/issues/3065">vscode#3065</a>
 * @see <a href="https://github.com/confluentinc/kafka-rest/blob/31952284c970fd764a666e92a519c3569008fabf/kafka-rest/src/main/java/io/confluent/kafkarest/entities/v3/ProduceRequest.java">ProduceRequest</a>
 */
@RegisterForReflection
public record ProduceRequest(
    @JsonProperty("partition_id") Integer partitionId,
    List<ProduceRequestHeader> headers,
    ProduceRequestData key,
    ProduceRequestData value,
    Date timestamp
) {

  public ProduceRequest withKey(ProduceRequestData key) {
    return new ProduceRequest(partitionId, headers, key, value, timestamp);
  }

  public ProduceRequest withValue(ProduceRequestData value) {
    return new ProduceRequest(partitionId, headers, key, value, timestamp);
  }
}
