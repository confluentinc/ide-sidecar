package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * When a new workspace connects, it first sends this WORKSPACE_HELLO message to the sidecar as indication
 * that the client is ready to go. Sidecar crosschecks the workspace (process) id, then stores the
 * websocket session into its map of happy sessions and then announces the connected workspace
 * count change to everyone.
 *
 * @param workspaceId The newly connected workspace id.
 */
@RegisterForReflection
public record HelloBody (
    @JsonProperty("workspace_id") long workspaceId
) implements MessageBody {
}