package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.cloud.scaffold.v1.model.ApplyScaffoldV1TemplateRequest;
import java.util.HashMap;
import java.util.Map;

public record ApplyTemplateRequest(
    @JsonProperty(required = true)
    Map<String, Object> options
) {
    public ApplyScaffoldV1TemplateRequest toApplyScaffoldV1TemplateRequest() {
        return ApplyScaffoldV1TemplateRequest
            .builder()
            .options(
                options
                    .entrySet()
                    .stream()
                    .collect(
                        HashMap::new,
                        (map, entry) -> map.put(
                            entry.getKey(), entry.getValue().toString()
                        ),
                        HashMap::putAll
                    )
            )
            .build();
    }
}
