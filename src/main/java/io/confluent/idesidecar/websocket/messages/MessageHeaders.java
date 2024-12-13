package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
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
    @NotNull @JsonProperty("message_type") String type,
    @NotNull @JsonProperty("originator") String originator,
    @NotNull @JsonProperty("message_id") String id
) {

    /** Constructor for outbound messages. */
    public MessageHeaders(String type, String originator) {
        this(type, originator, UUID.randomUUID().toString());
    }

    /** Constructor for deserialized messages. */
    @JsonCreator
    public MessageHeaders(
        @JsonProperty("message_type") String type,
        @JsonProperty("originator") String originator,
        @JsonProperty("message_id") String id) {
        this.type = type;
        this.originator = originator;
        this.id = id;
    }
}