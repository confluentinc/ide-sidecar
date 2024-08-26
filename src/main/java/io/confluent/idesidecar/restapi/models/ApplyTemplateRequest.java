package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record ApplyTemplateRequest(
    @JsonProperty(required = true)
    Map<String, Object> options
) {

}
