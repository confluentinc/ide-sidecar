package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ProduceRequestData(
    String type,
    String subject,
    @JsonProperty("subject_name_strategy") String subjectNameStrategy,
    @JsonProperty("schema_id") Integer schemaId,
    @JsonProperty("schema_version") Integer schemaVersion,
    String schema,
    @JsonInclude(Include.ALWAYS) Object data
) {
}
