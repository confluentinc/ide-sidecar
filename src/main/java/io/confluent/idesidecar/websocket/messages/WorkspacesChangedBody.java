package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Sent by the sidecar to the workspace when the list of workspaces has changed.
 */
@RegisterForReflection
public record WorkspacesChangedBody (
    @JsonProperty("current_workspace_count") int workspaceCount
) implements MessageBody {
}