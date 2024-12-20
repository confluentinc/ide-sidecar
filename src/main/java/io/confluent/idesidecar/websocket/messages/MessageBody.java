package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Base class for all websocket message bodies. As long as the message body types have distinct
 * fields, the {@link JsonTypeInfo.Id#DEDUCTION} strategy should be sufficient to deserialize them.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.DEDUCTION,
    defaultImpl = DynamicMessageBody.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = WorkspacesChangedBody.class),
    @JsonSubTypes.Type(value = ProtocolErrorBody.class),
    @JsonSubTypes.Type(value = HelloBody.class),
    @JsonSubTypes.Type(value = DynamicMessageBody.class),
})
@RegisterForReflection
public interface MessageBody {

}
