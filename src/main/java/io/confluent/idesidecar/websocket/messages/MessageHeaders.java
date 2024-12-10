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
 * @see ResponseMessageHeaders for a subclass adding an in-response-to-ish field, used when sidecar
 *                              is responding to a specific message.
 */
@RegisterForReflection
@JsonSerialize
public class MessageHeaders {

    /**
     * The type of message. Not an enumeration because the set of workspace<->workspace messages
     * is open-ended.
     */
    @NotNull
    @JsonProperty("message_type")
    public final String type;

    @NotNull
    public final Audience audience;

    /**
     * The originator of the message. For messages originating from the sidecar, this will always be
     * "sidecar". Otherwise will be the originating IDE workspace's process id.
     */
    @NotNull
    public final String originator;

    @NotNull
    @JsonProperty("message_id")
    public final String id;

    /** Constructor for outbound messages. */
    public MessageHeaders(String type, Audience audience, String originator) {
        this.type = type;
        this.audience = audience;
        this.originator = originator;

        this.id = UUID.randomUUID().toString();
    }

    /** Constructor for deserialized messages */
    @JsonCreator
    public MessageHeaders(@JsonProperty("message_type") String type,
                          @JsonProperty("audience") Audience audience,
                          @JsonProperty("originator") String originator,
                          @JsonProperty("message_id") String id) {
        this.type = type;
        this.audience = audience;
        this.originator = originator;
        this.id = id;
    }



}