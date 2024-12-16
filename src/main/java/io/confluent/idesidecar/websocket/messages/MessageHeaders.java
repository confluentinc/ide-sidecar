package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.UUID;
import javax.validation.constraints.NotNull;

/**
 * Structure describing the headers for all websocket messages.
 *
 * @see Message
 */
@RegisterForReflection
public record MessageHeaders(
    @NotNull @JsonProperty("message_type") MessageType messageType,
    @NotNull @JsonProperty("originator") String originator,
    @NotNull @JsonProperty("message_id") String id
) {
    /** Constructor for outbound messages. */
    public MessageHeaders(MessageType messageType, String originator) {
        this(messageType, originator, UUID.randomUUID().toString());
    }
}