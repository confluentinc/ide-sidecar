package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.UUID;
import javax.validation.constraints.NotNull;


/**
 * Structure describing the headers for all websocket messages.
 *
 * @see Message
 */
@RegisterForReflection
public class MessageHeaders {

    /**
     * The type of message. Not an enumeration because the set of workspace<->workspace messages
     * is open-ended.
     */
    @NotNull
    @JsonProperty("message_type")
    public final String type;

    /**
     * The originator of the message. Will either be "sidecar" or the originating IDE workspace's
     * process id, and should correspond with connection-level metadata already stored.
     */
    @NotNull
    @JsonProperty("originator")
    public final String originator;

    /**
     * A unique identifier for a message.
     */
    @NotNull
    @JsonProperty("message_id")
    public final String id;

    /** Constructor for outbound messages. */
    public MessageHeaders(String type, String originator) {
        this.type = type;
        this.originator = originator;
        this.id = UUID.randomUUID().toString();
    }

    /** Constructor for deserialized messages */
    @JsonCreator
    public MessageHeaders(@JsonProperty("message_type") String type,
                          @JsonProperty("originator") String originator,
                          @JsonProperty("message_id") String id) {
        this.type = type;
        this.originator = originator;
        this.id = id;
    }
}