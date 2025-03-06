package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SchemaDetails(
    @JsonProperty("schema_id")
    Integer schemaId,
    @JsonProperty("deserializer_tech")
    DeserializerTech deserializerTech
) {

    public SchemaDetails {
        if (deserializerTech == null) {
            throw new IllegalArgumentException("Deserializer tech must be specified");
        }

        if (schemaId != null && deserializerTech.isSchemaless()) {
            throw new IllegalArgumentException(
                "Schema ID and deserializer tech "
                    + "cannot be specified for RAW or JSON deserialization"
            );
        } else if (schemaId == null && !deserializerTech.isSchemaless()) {
            throw new IllegalArgumentException(
                "Schema ID must be specified for AVRO, JSON, or PROTOBUF deserialization"
            );
        }
    }
}
