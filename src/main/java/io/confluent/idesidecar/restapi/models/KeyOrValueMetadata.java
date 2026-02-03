package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record KeyOrValueMetadata(
    @JsonProperty("schema_id")
    Integer schemaId,
    @JsonProperty("schema_guid")
    UUID schemaGuid,
    @JsonProperty("data_format")
    DataFormat dataFormat
) {

  public KeyOrValueMetadata {
    if (dataFormat == null) {
      throw new IllegalArgumentException("Data format must be specified");
    }

    if (schemaId == null && schemaGuid == null && !dataFormat.isSchemaless()) {
      throw new IllegalArgumentException(
          "Schema ID or GUID must be specified for AVRO, JSONSCHEMA, or PROTOBUF deserialization"
      );
    }
  }
}
