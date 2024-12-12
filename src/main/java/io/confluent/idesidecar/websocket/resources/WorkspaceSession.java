package io.confluent.idesidecar.websocket.resources;

/** Record describing authorized workspace sessions: processId int, ... **/
public record WorkspaceSession(int processId) {
}