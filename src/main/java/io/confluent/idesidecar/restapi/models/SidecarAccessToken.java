package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record SidecarAccessToken(
    @JsonProperty(value = "auth_secret") String authSecret
) {


}
