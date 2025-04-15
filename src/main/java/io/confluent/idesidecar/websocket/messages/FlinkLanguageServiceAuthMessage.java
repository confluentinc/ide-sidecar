package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public record FlinkLanguageServiceAuthMessage (
    @JsonProperty(value = "Token") String token,
    @JsonProperty(value = "EnvironmentId") String environmentId,
    @JsonProperty(value = "OrganizationId") String organizationId
) {
}
