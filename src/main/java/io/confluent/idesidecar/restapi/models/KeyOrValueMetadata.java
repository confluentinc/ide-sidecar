package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record KeyOrValueMetadata(
    @JsonProperty("schema_id")
    Integer schemaId,
    @JsonProperty("data_format")
    DataFormat dataFormat
) {

    public KeyOrValueMetadata {
        if (dataFormat == null) {
            throw new IllegalArgumentException("Data format must be specified");
        }

        if (schemaId == null && !dataFormat.isSchemaless()) {
            throw new IllegalArgumentException(
                "Schema ID must be specified for AVRO, JSONSCHEMA, or PROTOBUF deserialization"
            );
        }
    }
}
