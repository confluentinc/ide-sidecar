package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Sent by the sidecar to the workspace when the list of workspaces has changed.
 */
@RegisterForReflection
public record ProtocolErrorBody(
    /* The error message. **/
    @JsonProperty("error") String error,
    /* Possible original message id that caused the error. May be null. */
    @JsonProperty("original_message_id") String originalMessageId
) implements MessageBody {

}