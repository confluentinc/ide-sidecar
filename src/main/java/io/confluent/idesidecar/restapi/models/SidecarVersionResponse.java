package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SidecarVersionResponse(
    @JsonProperty(value = "version") String version
) {

  // commit hash, build date to follow, once we have CICD embed them into the codebase
  // so that we can get at them, say via a resource loading mechanism?

}
