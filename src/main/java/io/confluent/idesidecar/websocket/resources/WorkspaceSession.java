package io.confluent.idesidecar.websocket.resources;

/** Record describing authorized workspace sessions: processId long, ... **/
public record WorkspaceSession(long processId) {
}