package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SidecarAccessToken(
    @JsonProperty(value = "auth_secret") String authSecret
) {


}
