package io.confluent.idesidecar.websocket.resources;

import io.quarkus.runtime.annotations.RegisterForReflection;

/** Record describing authorized workspace sessions: processId long, ... **/
@RegisterForReflection
public record WorkspaceSession(long processId) {
}