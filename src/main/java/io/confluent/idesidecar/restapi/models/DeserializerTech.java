package io.confluent.idesidecar.restapi.models;

import io.confluent.idesidecar.restapi.kafkarest.SchemaFormat;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description = """
    The deserializer technology used to interpret the bytes of a Kafka message.
    
    * AVRO: Apache Avro schema format.
    * JSONSCHEMA: JSON schema format.
    * PROTOBUF: Google Protocol Buffers schema format.
    * PARSED_JSON:
        Bytes parsed as JSON.
        The bytes did not contain a magic byte specifying a schema id, and the raw bytes
        were successfully parsed into a JSON value.
    * RAW_BYTES:
        Raw bytes. These are the scenarios where it would be used:
        - Arbitrary bytes that are NOT written/read using an implementation of Kafka serializer/deserializer.
        And further, we tried to but could not interpret these bytes as a JSON value.
        - The Kafka serializer/deserializer known to sidecar failed to parse the schematized bytes
        (meaning the schema id was present in the magic bytes, but our classes could not interpret the rest of the bytes.)
    """
)
public enum DeserializerTech {
  AVRO,
  JSONSCHEMA,
  PROTOBUF,
  PARSED_JSON,
  RAW_BYTES;

  /**
   * Get the DeserializerTech for the given schema format.
   */
  public static DeserializerTech fromSchemaFormat(SchemaFormat format) {
    return switch (format) {
      case SchemaFormat.AVRO -> AVRO;
      case SchemaFormat.JSON -> JSONSCHEMA;
      case SchemaFormat.PROTOBUF -> PROTOBUF;
    };
  }

  public boolean isSchemaless() {
    return this == RAW_BYTES || this == PARSED_JSON;
  }
}
